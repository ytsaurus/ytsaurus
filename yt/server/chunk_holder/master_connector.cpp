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

#include <ytlib/rpc/client.h>

#include <ytlib/misc/delayed_invoker.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/string.h>

#include <ytlib/meta_state/master_channel.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/node_tracker_client/node_statistics.h>

#include <server/cell_node/bootstrap.h>

#include <util/random/random.h>

namespace NYT {
namespace NChunkHolder {

using namespace NRpc;
using namespace NNodeTrackerClient;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& SILENT_UNUSED Logger = DataNodeLogger;

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
    // Create proxies.
    NodeProxy.Reset(new TNodeProxy(Bootstrap->GetMasterChannel()));
    JobProxy.Reset(new TJobProxy(Bootstrap->GetMasterChannel()));

    // Chunk store callbacks are always called in Control thread.
    Bootstrap->GetChunkStore()->SubscribeChunkAdded(
        BIND(&TMasterConnector::OnChunkAdded, MakeWeak(this)));
    Bootstrap->GetChunkStore()->SubscribeChunkRemoved(
        BIND(&TMasterConnector::OnChunkRemoved, MakeWeak(this)));

    Bootstrap->GetChunkCache()->SubscribeChunkAdded(
        BIND(&TMasterConnector::OnChunkAdded, MakeWeak(this))
            .Via(ControlInvoker));
    Bootstrap->GetChunkCache()->SubscribeChunkRemoved(
        BIND(&TMasterConnector::OnChunkRemoved, MakeWeak(this))
            .Via(ControlInvoker));

    TDelayedInvoker::Submit(
        BIND(&TMasterConnector::OnHeartbeat, MakeStrong(this))
            .Via(ControlInvoker),
        RandomDuration(Config->HeartbeatSplay));
}

void TMasterConnector::ForceRegister()
{
    VERIFY_THREAD_AFFINITY_ANY();

    ControlInvoker->Invoke(BIND(
        &TMasterConnector::DoForceRegister,
        MakeStrong(this)));
}

void TMasterConnector::DoForceRegister()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Disconnect();
    OnHeartbeat();
}

TNodeId TMasterConnector::GetNodeId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return NodeId;
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
            SendFullNodeHeartbeat();
            SendJobHeartbeat();
            break;
        case EState::Online:
            SendIncrementalNodeHeartbeat();
            SendJobHeartbeat();
            break;
        default:
            YUNREACHABLE();
    }
}

void TMasterConnector::SendRegister()
{
    auto request = NodeProxy->RegisterNode();
    *request->mutable_statistics() = ComputeStatistics();
    ToProto(request->mutable_node_descriptor(), Bootstrap->GetLocalDescriptor());
    ToProto(request->mutable_cell_guid(), Bootstrap->GetCellGuid());
    request->Invoke().Subscribe(
        BIND(&TMasterConnector::OnRegisterResponse, MakeStrong(this))
            .Via(ControlInvoker));

    LOG_INFO("Node register request sent (%s)",
        ~ToString(*request->mutable_statistics()));
}

NNodeTrackerClient::NProto::TNodeStatistics TMasterConnector::ComputeStatistics()
{
    NNodeTrackerClient::NProto::TNodeStatistics result;

    i64 totalAvailableSpace = 0;
    i64 totalUsedSpace = 0;
    int totalChunkCount = 0;
    int totalSessionCount = 0;
    bool full = true;

    FOREACH (auto location, Bootstrap->GetChunkStore()->Locations()) {
        auto* locationStatistics = result.add_locations();

        locationStatistics->set_available_space(location->GetAvailableSpace());
        locationStatistics->set_used_space(location->GetUsedSpace());
        locationStatistics->set_chunk_count(location->GetChunkCount());
        locationStatistics->set_session_count(location->GetSessionCount());
        locationStatistics->set_full(location->IsFull());
        locationStatistics->set_enabled(location->IsEnabled());

        if (location->IsEnabled()) {
            totalAvailableSpace += location->GetAvailableSpace();
            full &= location->IsFull();
        }

        totalUsedSpace += location->GetUsedSpace();
        totalChunkCount += location->GetChunkCount();
        totalSessionCount += location->GetSessionCount();
    }

    result.set_total_available_space(totalAvailableSpace);
    result.set_total_used_space(totalUsedSpace);
    result.set_total_chunk_count(totalChunkCount);
    result.set_total_session_count(totalSessionCount);
    result.set_full(full);

    return result;
}

void TMasterConnector::OnRegisterResponse(TNodeProxy::TRspRegisterNodePtr rsp)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!rsp->IsOK()) {
        Disconnect();
        ScheduleHeartbeat();

        LOG_WARNING(*rsp, "Error registering node");
        return;
    }

    auto cellGuid = FromProto<TGuid>(rsp->cell_guid());
    YCHECK(!cellGuid.IsEmpty());

    if (Bootstrap->GetCellGuid().IsEmpty()) {
        Bootstrap->UpdateCellGuid(cellGuid);
    }

    NodeId = rsp->node_id();
    State = EState::Registered;

    LOG_INFO("Successfully registered node (NodeId: %d)",
        NodeId);

    SendFullNodeHeartbeat();
}

void TMasterConnector::SendFullNodeHeartbeat()
{
    auto request = NodeProxy
        ->FullHeartbeat()
        ->SetCodec(NCompression::ECodec::Lz4)
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

    AddedSinceLastSuccess.clear();
    RemovedSinceLastSuccess.clear();

    request->Invoke().Subscribe(
        BIND(&TMasterConnector::OnFullNodeHeartbeatResponse, MakeStrong(this))
        .Via(ControlInvoker));

    LOG_INFO("Full node heartbeat sent (%s)", ~ToString(request->statistics()));
}

void TMasterConnector::SendIncrementalNodeHeartbeat()
{
    auto request = NodeProxy
        ->IncrementalHeartbeat()
        ->SetCodec(NCompression::ECodec::Lz4);

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

    request->Invoke().Subscribe(
        BIND(&TMasterConnector::OnIncrementalNodeHeartbeatResponse, MakeStrong(this))
        .Via(ControlInvoker));

    LOG_INFO("Incremental node heartbeat sent (%s, AddedChunks: %d, RemovedChunks: %d)",
        ~ToString(request->statistics()),
        static_cast<int>(request->added_chunks_size()),
        static_cast<int>(request->removed_chunks_size()));
}

NNodeTrackerClient::NProto::TChunkAddInfo TMasterConnector::GetAddInfo(TChunkPtr chunk)
{
    NNodeTrackerClient::NProto::TChunkAddInfo result;
    ToProto(result.mutable_chunk_id(), chunk->GetId());
    result.set_cached(chunk->GetLocation()->GetType() == ELocationType::Cache);
    *result.mutable_chunk_info() = chunk->GetInfo();
    return result;
}

NNodeTrackerClient::NProto::TChunkRemoveInfo TMasterConnector::GetRemoveInfo(TChunkPtr chunk)
{
    NNodeTrackerClient::NProto::TChunkRemoveInfo result;
    ToProto(result.mutable_chunk_id(), chunk->GetId());
    result.set_cached(chunk->GetLocation()->GetType() == ELocationType::Cache);
    return result;
}

void TMasterConnector::OnFullNodeHeartbeatResponse(TNodeProxy::TRspFullHeartbeatPtr rsp)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ScheduleHeartbeat();

    if (!rsp->IsOK()) {
        OnHeartbeatError(rsp->GetError());
        return;
    }

    LOG_INFO("Successfully reported full node heartbeat");

    State = EState::Online;
}

void TMasterConnector::OnIncrementalNodeHeartbeatResponse(TNodeProxy::TRspIncrementalHeartbeatPtr rsp)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    ScheduleHeartbeat();

    if (!rsp->IsOK()) {
        OnHeartbeatError(rsp->GetError());
        return;
    }

    LOG_INFO("Successfully reported incremental node heartbeat");

    TChunkSet newAddedSinceLastSuccess;
    FOREACH (const auto& id, AddedSinceLastSuccess) {
        if (ReportedAdded.find(id) == ReportedAdded.end()) {
            newAddedSinceLastSuccess.insert(id);
        }
    }
    AddedSinceLastSuccess.swap(newAddedSinceLastSuccess);

    TChunkSet newRemovedSinceLastSuccess;
    FOREACH (const auto& id, RemovedSinceLastSuccess) {
        if (ReportedRemoved.find(id) == ReportedRemoved.end()) {
            newRemovedSinceLastSuccess.insert(id);
        }
    }
    RemovedSinceLastSuccess.swap(newRemovedSinceLastSuccess);
}

void TMasterConnector::SendJobHeartbeat()
{
    auto request = JobProxy->Heartbeat();

    YCHECK(NodeId != InvalidNodeId);
    request->set_node_id(NodeId);

    auto jobs = Bootstrap->GetJobExecutor()->GetAllJobs();
    FOREACH (auto job, jobs) {
        auto* info = request->add_jobs();
        ToProto(info->mutable_job_id(), job->GetJobId());
        info->set_state(job->GetState());
        if (job->GetState() == EJobState::Failed) {
            ToProto(info->mutable_error(), job->GetError());
        }
    }

    request->Invoke().Subscribe(
        BIND(&TMasterConnector::OnJobHeartbeatResponse, MakeStrong(this))
            .Via(ControlInvoker));

    LOG_INFO("Job heartbeat sent (Jobs: %d)",
        static_cast<int>(request->jobs_size()));
}

void TMasterConnector::OnJobHeartbeatResponse(TJobProxy::TRspHeartbeatPtr rsp)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!rsp->IsOK()) {
        OnHeartbeatError(rsp->GetError());
        return;
    }

    LOG_INFO("Successfully reported job heartbeat");

    auto jobExecutor = Bootstrap->GetJobExecutor();

    FOREACH (const auto& jobInfo, rsp->jobs_to_stop()) {
        auto jobId = FromProto<TJobId>(jobInfo.job_id());
        auto job = jobExecutor->FindJob(jobId);
        if (!job) {
            LOG_WARNING("Request to stop a non-existing job (JobId: %s)",
                ~ToString(jobId));
            continue;
        }
        jobExecutor->StopJob(job);
    }

    FOREACH (const auto& startInfo, rsp->jobs_to_start()) {
        auto jobId = FromProto<TJobId>(startInfo.job_id());
        auto jobType = EJobType(startInfo.type());
        auto chunkId = FromProto<TChunkId>(startInfo.chunk_id());
        auto targets = FromProto<TNodeDescriptor>(startInfo.targets());
        jobExecutor->StartJob(
            jobType,
            jobId,
            chunkId,
            targets);
    }
}

void TMasterConnector::OnHeartbeatError(const TError& error)
{
    LOG_WARNING(error, "Error sending heartbeat");

    if (!IsRetriableError(error)) {
        Disconnect();
    }
}

void TMasterConnector::Disconnect()
{
    State = EState::Offline;
    NodeId = InvalidNodeId;
    ReportedAdded.clear();
    ReportedRemoved.clear();
    AddedSinceLastSuccess.clear();
    RemovedSinceLastSuccess.clear();
}

void TMasterConnector::OnChunkAdded(TChunkPtr chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (State == EState::Offline)
        return;

    NLog::TTaggedLogger Logger(DataNodeLogger);
    Logger.AddTag(Sprintf("ChunkId: %s, Location: %s",
        ~ToString(chunk->GetId()),
        ~chunk->GetLocation()->GetPath()));

    if (AddedSinceLastSuccess.find(chunk) != AddedSinceLastSuccess.end()) {
        LOG_DEBUG("Addition of chunk has already been registered");
        return;
    }

    if (RemovedSinceLastSuccess.find(chunk) != RemovedSinceLastSuccess.end()) {
        RemovedSinceLastSuccess.erase(chunk);
        LOG_DEBUG("Trying to add a chunk whose removal has been registered, canceling removal and addition");
        return;
    }

    LOG_DEBUG("Registered addition of chunk");

    AddedSinceLastSuccess.insert(chunk);
}

void TMasterConnector::OnChunkRemoved(TChunkPtr chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (State == EState::Offline)
        return;

    NLog::TTaggedLogger Logger(DataNodeLogger);
    Logger.AddTag(Sprintf("ChunkId: %s, Location: %s",
        ~ToString(chunk->GetId()),
        ~chunk->GetLocation()->GetPath()));

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
