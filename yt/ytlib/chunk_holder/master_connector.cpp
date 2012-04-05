#include "stdafx.h"
#include "master_connector.h"
#include "common.h"
#include "config.h"
#include "location.h"
#include "block_store.h"
#include "chunk.h"
#include "chunk_store.h"
#include "chunk_cache.h"
#include "session_manager.h"
#include "job_executor.h"
#include "bootstrap.h"

#include <ytlib/actions/bind.h>
#include <ytlib/rpc/client.h>
#include <ytlib/misc/delayed_invoker.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/string.h>
#include <ytlib/chunk_server/holder_statistics.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/logging/tagged_logger.h>

#include <util/system/hostname.h>
#include <util/random/random.h>

namespace NYT {
namespace NChunkHolder {

using namespace NChunkServer::NProto;
using namespace NChunkClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TMasterConnector::TMasterConnector(TChunkHolderConfigPtr config, TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
    , State(EState::Offline)
    , HolderId(InvalidHolderId)
{
    YASSERT(config);
    YASSERT(bootstrap);
}

void TMasterConnector::Start()
{
    Proxy.Reset(new TProxy(~Bootstrap->GetMasterChannel()));

    Bootstrap->GetChunkStore()->SubscribeChunkAdded(BIND(
        &TMasterConnector::OnChunkAdded,
        MakeWeak(this)));
    Bootstrap->GetChunkStore()->SubscribeChunkRemoved(BIND(
        &TMasterConnector::OnChunkRemoved,
        MakeWeak(this)));
    Bootstrap->GetChunkCache()->SubscribeChunkAdded(BIND(
        &TMasterConnector::OnChunkAdded,
        MakeWeak(this)));
    Bootstrap->GetChunkCache()->SubscribeChunkRemoved(BIND(
        &TMasterConnector::OnChunkRemoved,
        MakeWeak(this)));

    // TODO(panin): think about specializing RandomNumber<TDuration>
    auto randomDelay = TDuration::MicroSeconds(RandomNumber(Config->HeartbeatSplay.MicroSeconds()));
    TDelayedInvoker::Submit(
        BIND(&TMasterConnector::OnHeartbeat, MakeStrong(this))
        .Via(Bootstrap->GetControlInvoker()),
        randomDelay);
}

void TMasterConnector::ScheduleHeartbeat()
{
    TDelayedInvoker::Submit(
        BIND(&TMasterConnector::OnHeartbeat, MakeStrong(this))
        .Via(Bootstrap->GetControlInvoker()),
        Config->HeartbeatPeriod);
}

void TMasterConnector::OnHeartbeat()
{
    switch (State) {
        case EState::Offline:
            SendRegister();
            break;
        case EState::Registered:
            SendFullHeartbeat();
            break;
        case EState::Online:
            SendIncrementalHeartbeat();
            break;
        default:
            YUNREACHABLE();
    }
}

void TMasterConnector::SendRegister()
{
    auto request = Proxy->RegisterHolder();
    *request->mutable_statistics() = ComputeStatistics();
    request->set_address(Bootstrap->GetPeerAddress());
    *request->mutable_incarnation_id() = Bootstrap->GetIncarnationId().ToProto();
    request->Invoke()->Subscribe(
        BIND(&TMasterConnector::OnRegisterResponse, MakeStrong(this))
        .Via(Bootstrap->GetControlInvoker()));

    LOG_INFO("Register request sent (%s)",
        ~ToString(*request->mutable_statistics()));
}

NChunkServer::NProto::THolderStatistics TMasterConnector::ComputeStatistics()
{
    i64 availableSpace = 0;
    i64 usedSpace = 0;
    bool isFull = true;
    FOREACH (const auto& location, Bootstrap->GetChunkStore()->Locations()) {
        availableSpace += location->GetAvailableSpace();
        usedSpace += location->GetUsedSpace();
        if (!location->IsFull()) {
            isFull = false;
        }
    }

    THolderStatistics result;
    result.set_available_space(availableSpace);
    result.set_used_space(usedSpace);
    result.set_chunk_count(Bootstrap->GetChunkStore()->GetChunkCount());
    result.set_session_count(Bootstrap->GetSessionManager()->GetSessionCount());
    result.set_full(isFull);

    return result;
}

void TMasterConnector::OnRegisterResponse(TProxy::TRspRegisterHolder::TPtr response)
{
    if (!response->IsOK()) {
        Disconnect();

        LOG_WARNING("Error registering at master\n%s", ~response->GetError().ToString());
        return;
    }

    HolderId = response->holder_id();
    State = EState::Registered;

    LOG_INFO("Successfully registered at master (HolderId: %d)", HolderId);

    SendFullHeartbeat();
}

void TMasterConnector::SendFullHeartbeat()
{
    auto request = Proxy
        ->FullHeartbeat()
        ->SetTimeout(Config->FullHeartbeatTimeout);

    YASSERT(HolderId != InvalidHolderId);
    request->set_holder_id(HolderId);
    *request->mutable_statistics() = ComputeStatistics();

    FOREACH (const auto& chunk, Bootstrap->GetChunkStore()->GetChunks()) {
        *request->add_chunks() = GetAddInfo(~chunk);
    }

    FOREACH (const auto& chunk, Bootstrap->GetChunkCache()->GetChunks()) {
        *request->add_chunks() = GetAddInfo(~chunk);
    }

    request->Invoke()->Subscribe(
        BIND(&TMasterConnector::OnFullHeartbeatResponse, MakeStrong(this))
        .Via(Bootstrap->GetControlInvoker()));

    LOG_INFO("Full heartbeat sent (%s, Chunks: %d)",
        ~ToString(request->statistics()),
        static_cast<int>(request->chunks_size()));
}

void TMasterConnector::SendIncrementalHeartbeat()
{
    auto request = Proxy->IncrementalHeartbeat();

    YASSERT(HolderId != InvalidHolderId);
    request->set_holder_id(HolderId);
    *request->mutable_statistics() = ComputeStatistics();

    ReportedAdded = AddedSinceLastSuccess;
    ReportedRemoved = RemovedSinceLastSuccess;

    FOREACH (auto chunk, ReportedAdded) {
        *request->add_added_chunks() = GetAddInfo(~chunk);
    }

    FOREACH (auto chunk, ReportedRemoved) {
        *request->add_removed_chunks() = GetRemoveInfo(~chunk);
    }

    FOREACH (const auto& job, Bootstrap->GetJobExecutor()->GetAllJobs()) {
        auto* info = request->add_jobs();
        *info->mutable_job_id() = job->GetJobId().ToProto();
        info->set_state(job->GetState());
    }

    request->Invoke()->Subscribe(
        BIND(&TMasterConnector::OnIncrementalHeartbeatResponse, MakeStrong(this))
        .Via(Bootstrap->GetControlInvoker()));

    LOG_INFO("Incremental heartbeat sent (%s, AddedChunks: %d, RemovedChunks: %d, Jobs: %d)",
        ~ToString(request->statistics()),
        static_cast<int>(request->added_chunks_size()),
        static_cast<int>(request->removed_chunks_size()),
        static_cast<int>(request->jobs_size()));
}

TChunkAddInfo TMasterConnector::GetAddInfo(TChunkPtr chunk)
{
    TChunkAddInfo info;
    *info.mutable_chunk_id() = chunk->GetId().ToProto();
    info.set_cached(chunk->GetLocation()->GetType() == ELocationType::Cache);
    info.set_size(chunk->GetSize());
    return info;
}

TChunkRemoveInfo TMasterConnector::GetRemoveInfo(TChunkPtr chunk)
{
    TChunkRemoveInfo info;
    *info.mutable_chunk_id() = chunk->GetId().ToProto();
    info.set_cached(chunk->GetLocation()->GetType() == ELocationType::Cache);
    return info;
}

void TMasterConnector::OnFullHeartbeatResponse(TProxy::TRspFullHeartbeat::TPtr response)
{
    ScheduleHeartbeat();
    
    if (!response->IsOK()) {
        OnHeartbeatError(response->GetError());
        return;
    }

    LOG_INFO("Successfully reported full heartbeat to master");
    
    State = EState::Online;
}

void TMasterConnector::OnIncrementalHeartbeatResponse(TProxy::TRspIncrementalHeartbeat::TPtr response)
{
    ScheduleHeartbeat();

    if (!response->IsOK()) {
        OnHeartbeatError(response->GetError());
        return;
    }

    LOG_INFO("Successfully reported incremental heartbeat to master");

    TChunks newAddedSinceLastSuccess;
    FOREACH (const auto& id, AddedSinceLastSuccess) {
        if (ReportedAdded.find(id) == ReportedAdded.end()) {
            newAddedSinceLastSuccess.insert(id);
        }
    }
    AddedSinceLastSuccess.swap(newAddedSinceLastSuccess);

    TChunks newRemovedSinceLastSuccess;
    FOREACH (const auto& id, RemovedSinceLastSuccess) {
        if (ReportedRemoved.find(id) == ReportedRemoved.end()) {
            newRemovedSinceLastSuccess.insert(id);
        }
    }
    RemovedSinceLastSuccess.swap(newRemovedSinceLastSuccess);

    FOREACH (const auto& jobInfo, response->jobs_to_stop()) {
        auto jobId = TJobId::FromProto(jobInfo.job_id());
        auto job = Bootstrap->GetJobExecutor()->FindJob(jobId);
        if (!job) {
            LOG_WARNING("Request to stop a non-existing job (JobId: %s)",
                ~jobId.ToString());
            continue;
        }

        Bootstrap->GetJobExecutor()->StopJob(~job);
    }

    FOREACH (const auto& startInfo, response->jobs_to_start()) {
        auto chunkId = TChunkId::FromProto(startInfo.chunk_id());
        auto jobId = TJobId::FromProto(startInfo.job_id());
        auto jobType = EJobType(startInfo.type());

        auto chunk = Bootstrap->GetChunkStore()->FindChunk(chunkId);
        if (!chunk) {
            LOG_WARNING("Job request for non-existing chunk is ignored (ChunkId: %s, JobId: %s, JobType: %s)",
                ~chunkId.ToString(),
                ~jobId.ToString(),
                ~jobType.ToString());
            continue;
        }

        Bootstrap->GetJobExecutor()->StartJob(
            jobType,
            jobId,
            ~chunk,
            FromProto<Stroka>(startInfo.target_addresses()));
    }
}

void TMasterConnector::OnHeartbeatError(const TError& error)
{
    LOG_WARNING("Error sending heartbeat to master\n%s", ~error.ToString());

    // Don't panic upon getting Timeout, TransportError or Unavailable.
    auto errorCode = error.GetCode();
    if (errorCode != NRpc::EErrorCode::Timeout && 
        errorCode != NRpc::EErrorCode::TransportError && 
        errorCode != NRpc::EErrorCode::Unavailable)
    {
        Disconnect();
    }
}

void TMasterConnector::Disconnect()
{
    HolderId = InvalidHolderId;
    State = EState::Offline;
    ReportedAdded.clear();
    ReportedRemoved.clear();
    AddedSinceLastSuccess.clear();
    RemovedSinceLastSuccess.clear();
}

void TMasterConnector::OnChunkAdded(TChunkPtr chunk)
{
    NLog::TTaggedLogger Logger(ChunkHolderLogger);
    Logger.AddTag(Sprintf("ChunkId: %s, Location: %s",
        ~chunk->GetId().ToString(),
        ~chunk->GetLocation()->GetPath()));

    if (State != EState::Online)
        return;

    if (AddedSinceLastSuccess.find(chunk) != AddedSinceLastSuccess.end()) {
        LOG_DEBUG("Addition of chunk has already been registered");
        return;
    }

    if (RemovedSinceLastSuccess.find(chunk) != RemovedSinceLastSuccess.end()) {
        RemovedSinceLastSuccess.erase(chunk);
        LOG_DEBUG("Trying to add a chunk whose removal has been registered, cancelling removal and addition");
        return;
    }

    LOG_DEBUG("Registered addition of chunk");

    AddedSinceLastSuccess.insert(chunk);
}

void TMasterConnector::OnChunkRemoved(TChunkPtr chunk)
{
    NLog::TTaggedLogger Logger(ChunkHolderLogger);
    Logger.AddTag(Sprintf("ChunkId: %s, Location: %s",
        ~chunk->GetId().ToString(),
        ~chunk->GetLocation()->GetPath()));

    if (State != EState::Online)
        return;

    if (RemovedSinceLastSuccess.find(chunk) != RemovedSinceLastSuccess.end()) {
        LOG_DEBUG("Removal of chunk has already been registered");
        return;
    }

    if (AddedSinceLastSuccess.find(chunk) != AddedSinceLastSuccess.end()) {
        AddedSinceLastSuccess.erase(chunk);
        LOG_DEBUG("Trying to remove a chunk whose addition has been registered, canceling addition and removal");
        return;
    }

    LOG_DEBUG("Registered removal of chunk");

    RemovedSinceLastSuccess.insert(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
