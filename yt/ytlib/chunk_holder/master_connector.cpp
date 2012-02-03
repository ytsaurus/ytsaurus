#include "stdafx.h"
#include "master_connector.h"
#include "chunk_store.h"
#include "chunk_cache.h"
#include "session_manager.h"
#include "job_executor.h"

#include <ytlib/rpc/client.h>
#include <ytlib/election/cell_channel.h>
#include <ytlib/misc/delayed_invoker.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/string.h>
#include <ytlib/chunk_server/holder_statistics.h>
#include <ytlib/logging/tagged_logger.h>

#include <util/system/hostname.h>

namespace NYT {
namespace NChunkHolder {

using namespace NChunkServer::NProto;
using namespace NChunkClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TMasterConnector::TMasterConnector(TBootstrap* bootstrap)
    : Bootstrap(bootstrap)
    , Registered(false)
    , IncrementalHeartbeat(false)
    , HolderId(InvalidHolderId)
{
    YASSERT(bootstrap);

    auto config = bootstrap->GetConfig();
    auto channel = CreateCellChannel(~config->Masters);
    Proxy.Reset(new TProxy(~channel));
    Proxy->SetTimeout(config->MasterRpcTimeout);

    // TODO(babenko): use AsWeak
    bootstrap->GetChunkStore()->ChunkAdded().Subscribe(FromMethod(
        &TMasterConnector::OnChunkAdded,
        TWeakPtr<TMasterConnector>(this)));
    bootstrap->GetChunkStore()->ChunkRemoved().Subscribe(FromMethod(
        &TMasterConnector::OnChunkRemoved,
        TWeakPtr<TMasterConnector>(this)));
    bootstrap->GetChunkCache()->ChunkAdded().Subscribe(FromMethod(
        &TMasterConnector::OnChunkAdded,
        TWeakPtr<TMasterConnector>(this)));
    bootstrap->GetChunkCache()->ChunkRemoved().Subscribe(FromMethod(
        &TMasterConnector::OnChunkRemoved,
        TWeakPtr<TMasterConnector>(this)));

    LOG_INFO("Chunk holder address is %s, master addresses are [%s]",
        ~config->PeerAddress,
        ~JoinToString(config->Masters->Addresses));

    OnHeartbeat();
}

void TMasterConnector::ScheduleHeartbeat()
{
    TDelayedInvoker::Submit(
        ~FromMethod(&TMasterConnector::OnHeartbeat, TPtr(this))
        ->Via(Bootstrap->GetServiceInvoker()),
        Bootstrap->GetConfig()->HeartbeatPeriod);
}

void TMasterConnector::OnHeartbeat()
{
    if (Registered) {
        SendHeartbeat();
    } else {
        SendRegister();
    }
}

void TMasterConnector::SendRegister()
{
    auto request = Proxy->RegisterHolder();
    
    *request->mutable_statistics() = ComputeStatistics();

    request->set_address(Bootstrap->GetConfig()->PeerAddress);
    request->set_incarnation_id(Bootstrap->GetIncarnationId().ToProto());

    request->Invoke()->Subscribe(
        FromMethod(&TMasterConnector::OnRegisterResponse, TPtr(this))
        ->Via(Bootstrap->GetServiceInvoker()));

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
    ScheduleHeartbeat();

    if (!response->IsOK()) {
        OnDisconnected();

        LOG_WARNING("Error registering at master\n%s", ~response->GetError().ToString());
        return;
    }

    HolderId = response->holder_id();
    Registered = true;
    IncrementalHeartbeat = false;

    LOG_INFO("Successfully registered at master (HolderId: %d)",
        HolderId);
}

void TMasterConnector::SendHeartbeat()
{
    auto request = Proxy->HolderHeartbeat();

    YASSERT(HolderId != InvalidHolderId);
    request->set_holder_id(HolderId);

    *request->mutable_statistics() = ComputeStatistics();
    const auto& statistics = *request->mutable_statistics();

    if (IncrementalHeartbeat) {
        ReportedAdded = AddedSinceLastSuccess;
        ReportedRemoved = RemovedSinceLastSuccess;

        FOREACH (auto chunk, ReportedAdded) {
            *request->add_added_chunks() = GetAddInfo(~chunk);
        }

        FOREACH (auto chunk, ReportedRemoved) {
            *request->add_removed_chunks() = GetRemoveInfo(~chunk);
        }
    } else {
        FOREACH (const auto& chunk, Bootstrap->GetChunkStore()->GetChunks()) {
            *request->add_added_chunks() = GetAddInfo(~chunk);
        }

        FOREACH (const auto& chunk, Bootstrap->GetChunkCache()->GetChunks()) {
            *request->add_added_chunks() = GetAddInfo(~chunk);
        }
    }

    FOREACH (const auto& job, Bootstrap->GetJobExecutor()->GetAllJobs()) {
        auto* info = request->add_jobs();
        info->set_job_id(job->GetJobId().ToProto());
        info->set_state(job->GetState());
    }

    request->Invoke()->Subscribe(
        FromMethod(&TMasterConnector::OnHeartbeatResponse, TPtr(this))
        ->Via(Bootstrap->GetServiceInvoker()));

    LOG_DEBUG("Heartbeat sent (%s, AddedChunks: %d, RemovedChunks: %d, Jobs: %d)",
        ~ToString(statistics),
        static_cast<int>(request->added_chunks_size()),
        static_cast<int>(request->removed_chunks_size()),
        static_cast<int>(request->jobs_size()));
}

TReqHolderHeartbeat::TChunkAddInfo TMasterConnector::GetAddInfo(const TChunk* chunk)
{
    TReqHolderHeartbeat::TChunkAddInfo info;
    info.set_chunk_id(chunk->GetId().ToProto());
    info.set_cached(chunk->GetLocation()->GetType() == ELocationType::Cache);
    info.set_size(chunk->GetSize());
    return info;
}

TReqHolderHeartbeat::TChunkRemoveInfo TMasterConnector::GetRemoveInfo(const TChunk* chunk)
{
    TReqHolderHeartbeat::TChunkRemoveInfo info;
    info.set_chunk_id(chunk->GetId().ToProto());
    info.set_cached(chunk->GetLocation()->GetType() == ELocationType::Cache);
    return info;
}

void TMasterConnector::OnHeartbeatResponse(TProxy::TRspHolderHeartbeat::TPtr response)
{
    ScheduleHeartbeat();
    
    auto errorCode = response->GetErrorCode();
    if (errorCode != NYT::TError::OK) {
        LOG_WARNING("Error sending heartbeat to master\n%s", ~response->GetError().ToString());

        // Don't panic upon getting Timeout, TransportError or Unavailable.
        if (errorCode != NRpc::EErrorCode::Timeout && 
            errorCode != NRpc::EErrorCode::TransportError && 
            errorCode != NRpc::EErrorCode::Unavailable)
        {
            OnDisconnected();
        }

        return;
    }

    LOG_INFO("Successfully reported heartbeat to master");

    if (IncrementalHeartbeat) {
        TChunks newAddedSinceLastSuccess;
        TChunks newRemovedSinceLastSuccess;

        FOREACH (const auto& id, AddedSinceLastSuccess) {
            if (ReportedAdded.find(id) == ReportedAdded.end()) {
                newAddedSinceLastSuccess.insert(id);
            }
        }

        FOREACH (const auto& id, RemovedSinceLastSuccess) {
            if (ReportedRemoved.find(id) == ReportedRemoved.end()) {
                newRemovedSinceLastSuccess.insert(id);
            }
        }

        AddedSinceLastSuccess.swap(newAddedSinceLastSuccess);
        RemovedSinceLastSuccess.swap(newRemovedSinceLastSuccess);
    } else {
        IncrementalHeartbeat = true;
    }

    FOREACH (const auto& jobProtoId, response->jobs_to_stop()) {
        auto jobId = TJobId::FromProto(jobProtoId);
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

void TMasterConnector::OnDisconnected()
{
    HolderId = InvalidHolderId;
    Registered = false;
    IncrementalHeartbeat = false;
    ReportedAdded.clear();
    ReportedRemoved.clear();
    AddedSinceLastSuccess.clear();
    RemovedSinceLastSuccess.clear();
}

void TMasterConnector::OnChunkAdded(TChunk* chunk)
{
    NLog::TTaggedLogger Logger(ChunkHolderLogger);
    Logger.AddTag(Sprintf("ChunkId: %s, Location: %s",
        ~chunk->GetId().ToString(),
        ~chunk->GetLocation()->GetPath()));

    if (!IncrementalHeartbeat)
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

void TMasterConnector::OnChunkRemoved(TChunk* chunk)
{
    NLog::TTaggedLogger Logger(ChunkHolderLogger);
    Logger.AddTag(Sprintf("ChunkId: %s, Location: %s",
        ~chunk->GetId().ToString(),
        ~chunk->GetLocation()->GetPath()));

    if (!IncrementalHeartbeat)
        return;

    if (RemovedSinceLastSuccess.find(chunk) != RemovedSinceLastSuccess.end()) {
        LOG_DEBUG("Removal of chunk has already been registered");
        return;
    }

    if (AddedSinceLastSuccess.find(chunk) != AddedSinceLastSuccess.end()) {
        AddedSinceLastSuccess.erase(chunk);
        LOG_DEBUG("Trying to remove a chunk whose addition has been registered, cancelling addition and removal");
        return;
    }

    LOG_DEBUG("Registered removal of chunk");

    RemovedSinceLastSuccess.insert(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
