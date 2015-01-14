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

#include <core/rpc/client.h>

#include <core/concurrency/delayed_executor.h>
#include <core/misc/serialize.h>
#include <core/misc/string.h>

#include <ytlib/meta_state/master_channel.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/node_tracker_client/node_statistics.h>
#include <ytlib/node_tracker_client/helpers.h>

#include <server/job_agent/job_controller.h>

#include <server/cell_node/bootstrap.h>

#include <util/random/random.h>

namespace NYT {
namespace NDataNode {

using namespace NRpc;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient;
using namespace NJobTrackerClient::NProto;
using namespace NCellNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TMasterConnector::TMasterConnector(TDataNodeConfigPtr config, TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
    , Started(false)
    , ControlInvoker(bootstrap->GetControlInvoker())
    , State(EState::Offline)
    , NodeId(InvalidNodeId)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker, ControlThread);
    YCHECK(Config);
    YCHECK(Bootstrap);
}

void TMasterConnector::Start()
{
    YCHECK(!Started);

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

    TDelayedExecutor::Submit(
        BIND(&TMasterConnector::StartHeartbeats, MakeStrong(this))
            .Via(ControlInvoker),
        RandomDuration(Config->IncrementalHeartbeatPeriod));

    Started = true;
}

void TMasterConnector::ForceRegister()
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!Started)
        return;

    ControlInvoker->Invoke(BIND(
        &TMasterConnector::StartHeartbeats,
        MakeStrong(this)));
}

void TMasterConnector::StartHeartbeats()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Reset();
    SendRegister();
}

bool TMasterConnector::IsConnected() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return NodeId != InvalidNodeId;
}

TNodeId TMasterConnector::GetNodeId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return NodeId;
}

void TMasterConnector::RegisterAlert(const Stroka& alert)
{
    VERIFY_THREAD_AFFINITY_ANY();
    
    TGuard<TSpinLock> guard(AlertsLock);
    Alerts.push_back(alert);
}

void TMasterConnector::ScheduleNodeHeartbeat()
{
    TDelayedExecutor::Submit(
        BIND(&TMasterConnector::OnNodeHeartbeat, MakeStrong(this))
            .Via(HeartbeatInvoker),
        Config->IncrementalHeartbeatPeriod);
}

void TMasterConnector::ScheduleJobHeartbeat()
{
    TDelayedExecutor::Submit(
        BIND(&TMasterConnector::OnJobHeartbeat, MakeStrong(this))
            .Via(HeartbeatInvoker),
        Config->IncrementalHeartbeatPeriod);
}

void TMasterConnector::ResetAndScheduleRegister()
{
    Reset();

    TDelayedExecutor::Submit(
        BIND(&TMasterConnector::SendRegister, MakeStrong(this))
            .Via(HeartbeatInvoker),
        Config->IncrementalHeartbeatPeriod);
}

void TMasterConnector::OnNodeHeartbeat()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    switch (State) {
        case EState::Registered:
            SendFullNodeHeartbeat();
            break;
        case EState::Online:
            SendIncrementalNodeHeartbeat();
            break;
        default:
            YUNREACHABLE();
    }
}

void TMasterConnector::SendRegister()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TNodeTrackerServiceProxy proxy(Bootstrap->GetMasterChannel());
    auto req = proxy.RegisterNode();
    *req->mutable_statistics() = ComputeStatistics();
    ToProto(req->mutable_node_descriptor(), Bootstrap->GetLocalDescriptor());
    ToProto(req->mutable_cell_guid(), Bootstrap->GetCellGuid());
    req->Invoke().Subscribe(
        BIND(&TMasterConnector::OnRegisterResponse, MakeStrong(this))
            .Via(HeartbeatInvoker));

    State = EState::Registering;

    LOG_INFO("Node register request sent to master (%s)",
        ~ToString(*req->mutable_statistics()));
}

TNodeStatistics TMasterConnector::ComputeStatistics()
{
    TNodeStatistics result;

    i64 totalAvailableSpace = 0;
    i64 totalLowWatermarkSpace = 0;
    i64 totalUsedSpace = 0;
    int totalChunkCount = 0;
    int totalSessionCount = 0;
    bool full = true;

    auto chunkStore = Bootstrap->GetChunkStore();
    FOREACH (auto location, chunkStore->Locations()) {
        auto* locationStatistics = result.add_locations();

        locationStatistics->set_available_space(location->GetAvailableSpace());
        locationStatistics->set_used_space(location->GetUsedSpace());
        locationStatistics->set_chunk_count(location->GetChunkCount());
        locationStatistics->set_session_count(location->GetSessionCount());
        locationStatistics->set_full(location->IsFull());
        locationStatistics->set_enabled(location->IsEnabled());

        if (location->IsEnabled()) {
            totalAvailableSpace += location->GetAvailableSpace();
            totalLowWatermarkSpace += location->GetLowWatermarkSpace();
            full &= location->IsFull();
        }

        totalUsedSpace += location->GetUsedSpace();
        totalChunkCount += location->GetChunkCount();
        totalSessionCount += location->GetSessionCount();
    }

    result.set_total_available_space(totalAvailableSpace);
    result.set_total_low_watermark_space(totalLowWatermarkSpace);
    result.set_total_used_space(totalUsedSpace);
    result.set_total_chunk_count(totalChunkCount);
    result.set_full(full);

    auto sessionManager = Bootstrap->GetSessionManager();
    result.set_total_user_session_count(sessionManager->GetSessionCount(EWriteSessionType::User));
    result.set_total_replication_session_count(sessionManager->GetSessionCount(EWriteSessionType::Replication));
    result.set_total_repair_session_count(sessionManager->GetSessionCount(EWriteSessionType::Repair));

    return result;
}

void TMasterConnector::OnRegisterResponse(TNodeTrackerServiceProxy::TRspRegisterNodePtr rsp)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!rsp->IsOK()) {
        LOG_WARNING(*rsp, "Error registering node");
        ResetAndScheduleRegister();
        return;
    }

    auto cellGuid = FromProto<TGuid>(rsp->cell_guid());
    YCHECK(!cellGuid.IsEmpty());

    if (Bootstrap->GetCellGuid().IsEmpty()) {
        Bootstrap->UpdateCellGuid(cellGuid);
    }

    NodeId = rsp->node_id();
    YCHECK(State == EState::Registering);
    State = EState::Registered;

    LOG_INFO("Successfully registered node at master (NodeId: %d)",
        NodeId);

    SendFullNodeHeartbeat();
}

void TMasterConnector::SendFullNodeHeartbeat()
{
    TNodeTrackerServiceProxy proxy(Bootstrap->GetMasterChannel());
    auto request = proxy.FullHeartbeat()
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
            .Via(HeartbeatInvoker));

    LOG_INFO("Full node heartbeat sent to master (%s)", ~ToString(request->statistics()));
}

void TMasterConnector::SendIncrementalNodeHeartbeat()
{
    TNodeTrackerServiceProxy proxy(Bootstrap->GetMasterChannel());
    auto request = proxy.IncrementalHeartbeat()
        ->SetCodec(NCompression::ECodec::Lz4);

    YCHECK(NodeId != InvalidNodeId);
    request->set_node_id(NodeId);

    *request->mutable_statistics() = ComputeStatistics();

    {
        TGuard<TSpinLock> guard(AlertsLock);
        ToProto(request->mutable_alerts(), Alerts);
    }

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
            .Via(HeartbeatInvoker));

    LOG_INFO("Incremental node heartbeat sent to master (%s, AddedChunks: %d, RemovedChunks: %d)",
        ~ToString(request->statistics()),
        static_cast<int>(request->added_chunks_size()),
        static_cast<int>(request->removed_chunks_size()));
}

TChunkAddInfo TMasterConnector::GetAddInfo(TChunkPtr chunk)
{
    TChunkAddInfo result;
    ToProto(result.mutable_chunk_id(), chunk->GetId());
    result.set_cached(chunk->GetLocation()->GetType() == ELocationType::Cache);
    *result.mutable_chunk_info() = chunk->GetInfo();
    return result;
}

TChunkRemoveInfo TMasterConnector::GetRemoveInfo(TChunkPtr chunk)
{
    TChunkRemoveInfo result;
    ToProto(result.mutable_chunk_id(), chunk->GetId());
    result.set_cached(chunk->GetLocation()->GetType() == ELocationType::Cache);
    return result;
}

void TMasterConnector::OnFullNodeHeartbeatResponse(TNodeTrackerServiceProxy::TRspFullHeartbeatPtr rsp)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!rsp->IsOK()) {
        auto error = rsp->GetError();
        LOG_WARNING(error, "Error reporting full node heartbeat to master");
        if (IsRetriableError(error)) {
            ScheduleNodeHeartbeat();
        } else {
            ResetAndScheduleRegister();
        }
        return;
    }

    LOG_INFO("Successfully reported full node heartbeat to master");

    // Schedule another full heartbeat.
    if (Config->FullHeartbeatPeriod) {
        TDelayedExecutor::Submit(
            BIND(&TMasterConnector::StartHeartbeats, MakeStrong(this))
                .Via(HeartbeatInvoker),
            RandomDuration(*Config->FullHeartbeatPeriod));
    }

    State = EState::Online;

    SendJobHeartbeat();
    ScheduleNodeHeartbeat();
}

void TMasterConnector::OnIncrementalNodeHeartbeatResponse(TNodeTrackerServiceProxy::TRspIncrementalHeartbeatPtr rsp)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!rsp->IsOK()) {
        auto error = rsp->GetError();
        LOG_WARNING(error, "Error reporting incremental node heartbeat to master");
        if (IsRetriableError(error)) {
            ScheduleNodeHeartbeat();
        } else {
            ResetAndScheduleRegister();
        }
        return;
    }

    LOG_INFO("Successfully reported incremental node heartbeat to master");

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

    ScheduleNodeHeartbeat();
}

void TMasterConnector::OnJobHeartbeat()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    
    SendJobHeartbeat();
}

void TMasterConnector::SendJobHeartbeat()
{
    YCHECK(NodeId != InvalidNodeId);
    YCHECK(State == EState::Online);

    TJobTrackerServiceProxy proxy(Bootstrap->GetMasterChannel());
    auto req = proxy.Heartbeat();
    auto jobController = Bootstrap->GetJobController();
    jobController->PrepareHeartbeat(~req);

    req->Invoke().Subscribe(
        BIND(&TMasterConnector::OnJobHeartbeatResponse, MakeStrong(this))
            .Via(HeartbeatInvoker));

    LOG_INFO("Job heartbeat sent to master (ResourceUsage: {%s})",
        ~FormatResourceUsage(req->resource_usage(), req->resource_limits()));
}

void TMasterConnector::OnJobHeartbeatResponse(TJobTrackerServiceProxy::TRspHeartbeatPtr rsp)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!rsp->IsOK()) {
        auto error = rsp->GetError();
        LOG_WARNING(error, "Error reporting job heartbeat to master");
        if (IsRetriableError(error)) {
            ScheduleJobHeartbeat();
        } else {
            ResetAndScheduleRegister();
        }
        return;
    }

    LOG_INFO("Successfully reported job heartbeat to master");
    
    auto jobController = Bootstrap->GetJobController();
    jobController->ProcessHeartbeat(~rsp);

    ScheduleJobHeartbeat();
}

void TMasterConnector::Reset()
{
    if (HeartbeatContext) {
        HeartbeatContext->Cancel();
    }

    HeartbeatContext = New<TCancelableContext>();
    HeartbeatInvoker = HeartbeatContext->CreateInvoker(ControlInvoker);

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
        LOG_DEBUG("Trying to add a chunk whose removal has been registered, canceling removal");
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
        LOG_DEBUG("Trying to remove a chunk whose addition has been registered, canceling addition");
        AddedSinceLastSuccess.erase(chunk);
    }

    LOG_DEBUG("Registered removal of chunk");

    RemovedSinceLastSuccess.insert(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
