#include "stdafx.h"
#include "master_connector.h"
#include "private.h"
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
#include <server/chunk_server/node_statistics.h>
#include <ytlib/meta_state/master_channel.h>
#include <ytlib/logging/tagged_logger.h>

#include <util/random/random.h>

namespace NYT {
namespace NChunkHolder {

using namespace NChunkServer::NProto;
using namespace NChunkClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TMasterConnector::TMasterConnector(TDataNodeConfigPtr config, TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
    , ControlInvoker(bootstrap->GetControlInvoker())
    , State(EState::Offline)
    , NodeId(InvalidNodeId)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker, ControlThread);

    YCHECK(config);
    YCHECK(bootstrap);
}

void TMasterConnector::Start()
{
    Proxy.Reset(new TProxy(Bootstrap->GetMasterChannel()));

    // Chunk store callbacks are always called in Control thead.
    Bootstrap->GetChunkStore()->SubscribeChunkAdded(BIND(
        &TMasterConnector::OnChunkAdded,
        MakeWeak(this)));
    Bootstrap->GetChunkStore()->SubscribeChunkRemoved(BIND(
        &TMasterConnector::OnChunkRemoved,
        MakeWeak(this)));

    Bootstrap->GetChunkCache()->SubscribeChunkAdded(BIND(
        &TMasterConnector::OnChunkAdded,
        MakeWeak(this)).Via(ControlInvoker));
    Bootstrap->GetChunkCache()->SubscribeChunkRemoved(BIND(
        &TMasterConnector::OnChunkRemoved,
        MakeWeak(this)).Via(ControlInvoker));

    TDelayedInvoker::Submit(
        BIND(&TMasterConnector::OnHeartbeat, MakeStrong(this))
        .Via(ControlInvoker),
        RandomDuration(Config->HeartbeatSplay));
}

void TMasterConnector::ScheduleHeartbeat()
{
    TDelayedInvoker::Submit(
        BIND(&TMasterConnector::OnHeartbeat, MakeStrong(this))
        .Via(ControlInvoker),
        Config->HeartbeatPeriod);
}

void TMasterConnector::OnHeartbeat()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
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
    auto request = Proxy->RegisterNode();
    *request->mutable_statistics() = ComputeStatistics();
    request->set_address(Bootstrap->GetPeerAddress());
    *request->mutable_incarnation_id() = Bootstrap->GetIncarnationId().ToProto();
    *request->mutable_cell_guid() = Bootstrap->GetCellGuid().ToProto();
    request->Invoke().Subscribe(
        BIND(&TMasterConnector::OnRegisterResponse, MakeStrong(this))
        .Via(ControlInvoker));

    LOG_INFO("Register request sent (%s)",
        ~ToString(*request->mutable_statistics()));
}

NChunkServer::NProto::TNodeStatistics TMasterConnector::ComputeStatistics()
{
    TNodeStatistics nodeStatistics;
    
    i64 totalAvailableSpace = 0;
    i64 totalUsedSpace = 0;
    int totalChunkCount = 0;
    int totalSessionCount = 0;
    bool full = true;
    
    FOREACH (auto location, Bootstrap->GetChunkStore()->Locations()) {
        auto* locationStatistics = nodeStatistics.add_locations();

        locationStatistics->set_available_space(location->GetAvailableSpace());
        locationStatistics->set_used_space(location->GetUsedSpace());
        locationStatistics->set_chunk_count(location->GetChunkCount());
        locationStatistics->set_session_count(location->GetSessionCount());
        locationStatistics->set_full(location->IsFull());
        locationStatistics->set_enabled(true);

        totalAvailableSpace += location->GetAvailableSpace();
        totalUsedSpace += location->GetUsedSpace();
        totalChunkCount += location->GetChunkCount();
        totalSessionCount += location->GetSessionCount();
        full &= location->IsFull();
    }

    nodeStatistics.set_total_available_space(totalAvailableSpace);
    nodeStatistics.set_total_used_space(totalUsedSpace);
    nodeStatistics.set_total_chunk_count(totalChunkCount);
    nodeStatistics.set_total_session_count(totalSessionCount);
    nodeStatistics.set_full(full);

    return nodeStatistics;
}

void TMasterConnector::OnRegisterResponse(TProxy::TRspRegisterNodePtr response)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!response->IsOK()) {
        Disconnect();
        ScheduleHeartbeat();

        LOG_WARNING(*response, "Error registering at master");
        return;
    }

    auto cellGuid = TGuid::FromProto(response->cell_guid());
    YCHECK(!cellGuid.IsEmpty());

    if (Bootstrap->GetCellGuid().IsEmpty()) {
        Bootstrap->UpdateCellGuid(cellGuid);
    }

    NodeId = response->node_id();
    State = EState::Registered;

    LOG_INFO("Successfully registered at master (NodeId: %d)", NodeId);

    SendFullHeartbeat();
}

void TMasterConnector::SendFullHeartbeat()
{
    auto request = Proxy
        ->FullHeartbeat()
        ->SetCodec(ECodecId::Lz4)
        ->SetTimeout(Config->FullHeartbeatTimeout);

    YCHECK(NodeId != InvalidNodeId);
    request->set_node_id(NodeId);
    *request->mutable_statistics() = ComputeStatistics();

    FOREACH (const auto& chunk, Bootstrap->GetChunkStore()->GetChunks()) {
        *request->add_chunks() = GetAddInfo(chunk);
    }

    FOREACH (const auto& chunk, Bootstrap->GetChunkCache()->GetChunks()) {
        *request->add_chunks() = GetAddInfo(chunk);
    }

    request->Invoke().Subscribe(
        BIND(&TMasterConnector::OnFullHeartbeatResponse, MakeStrong(this))
        .Via(ControlInvoker));

    LOG_INFO("Full heartbeat sent (%s)", ~ToString(request->statistics()));
}

void TMasterConnector::SendIncrementalHeartbeat()
{
    auto request = Proxy
        ->IncrementalHeartbeat()
        ->SetCodec(ECodecId::Lz4);

    YCHECK(NodeId != InvalidNodeId);
    request->set_node_id(NodeId);
    *request->mutable_statistics() = ComputeStatistics();

    ReportedAdded = AddedSinceLastSuccess;
    ReportedRemoved = RemovedSinceLastSuccess;

    FOREACH (auto chunk, ReportedAdded) {
        *request->add_added_chunks() = GetAddInfo(chunk);
    }

    FOREACH (auto chunk, ReportedRemoved) {
        *request->add_removed_chunks() = GetRemoveInfo(chunk);
    }

    FOREACH (const auto& job, Bootstrap->GetJobExecutor()->GetAllJobs()) {
        auto* info = request->add_jobs();
        *info->mutable_job_id() = job->GetJobId().ToProto();
        info->set_state(job->GetState());
    }

    request->Invoke().Subscribe(
        BIND(&TMasterConnector::OnIncrementalHeartbeatResponse, MakeStrong(this))
        .Via(ControlInvoker));

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
    *info.mutable_chunk_info() = chunk->GetInfo();
    return info;
}

TChunkRemoveInfo TMasterConnector::GetRemoveInfo(TChunkPtr chunk)
{
    TChunkRemoveInfo info;
    *info.mutable_chunk_id() = chunk->GetId().ToProto();
    info.set_cached(chunk->GetLocation()->GetType() == ELocationType::Cache);
    return info;
}

void TMasterConnector::OnFullHeartbeatResponse(TProxy::TRspFullHeartbeatPtr response)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ScheduleHeartbeat();
    
    if (!response->IsOK()) {
        OnHeartbeatError(response->GetError());
        return;
    }

    LOG_INFO("Successfully reported full heartbeat to master");
    
    State = EState::Online;
}

void TMasterConnector::OnIncrementalHeartbeatResponse(TProxy::TRspIncrementalHeartbeatPtr response)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
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

        Bootstrap->GetJobExecutor()->StopJob(job);
    }

    FOREACH (const auto& startInfo, response->jobs_to_start()) {
        auto jobId = TJobId::FromProto(startInfo.job_id());
        auto jobType = EJobType(startInfo.type());
        auto chunkId = TChunkId::FromProto(startInfo.chunk_id());
        auto targetAddresses = FromProto<Stroka>(startInfo.target_addresses());
        Bootstrap->GetJobExecutor()->StartJob(
            jobType,
            jobId,
            chunkId,
            targetAddresses);
    }
}

void TMasterConnector::OnHeartbeatError(const TError& error)
{
    LOG_WARNING(error, "Error sending heartbeat to master");

    if (!IsRetriableError(error)) {
        Disconnect();
    }
}

void TMasterConnector::Disconnect()
{
    NodeId = InvalidNodeId;
    State = EState::Offline;
    ReportedAdded.clear();
    ReportedRemoved.clear();
    AddedSinceLastSuccess.clear();
    RemovedSinceLastSuccess.clear();
}

void TMasterConnector::OnChunkAdded(TChunkPtr chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    NLog::TTaggedLogger Logger(DataNodeLogger);
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
    VERIFY_THREAD_AFFINITY(ControlThread);

    NLog::TTaggedLogger Logger(DataNodeLogger);
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
