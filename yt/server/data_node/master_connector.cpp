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
#include "artifact.h"

#include <core/rpc/client.h>

#include <core/concurrency/delayed_executor.h>

#include <core/misc/serialize.h>
#include <core/misc/string.h>

#include <core/ytree/convert.h>

#include <ytlib/hydra/peer_channel.h>

#include <ytlib/hive/cell_directory.h>

#include <ytlib/election/config.h>

#include <ytlib/node_tracker_client/node_statistics.h>
#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/api/connection.h>
#include <ytlib/api/client.h>

#include <server/misc/memory_usage_tracker.h>

#include <server/job_agent/job_controller.h>

#include <server/tablet_node/slot_manager.h>
#include <server/tablet_node/tablet_slot.h>
#include <server/tablet_node/tablet.h>

#include <server/data_node/journal_dispatcher.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

#include <util/random/random.h>

namespace NYT {
namespace NDataNode {

using namespace NYTree;
using namespace NElection;
using namespace NRpc;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient;
using namespace NJobTrackerClient::NProto;
using namespace NTabletNode;
using namespace NHydra;
using namespace NHive;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NCellNode;

using NNodeTrackerClient::TAddressMap;
using NNodeTrackerClient::TNodeDescriptor;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TMasterConnector::TMasterConnector(
    TDataNodeConfigPtr config,
    const TAddressMap& localAddresses,
    TBootstrap* bootstrap)
    : Config_(config)
    , LocalAddresses_(localAddresses)
    , Bootstrap_(bootstrap)
    , ControlInvoker_(bootstrap->GetControlInvoker())
    , LocalDescriptor_(LocalAddresses_)
{
    VERIFY_INVOKER_THREAD_AFFINITY(ControlInvoker_, ControlThread);
    YCHECK(Config_);
    YCHECK(Bootstrap_);
}

void TMasterConnector::Start()
{
    YCHECK(!Started_);

    Bootstrap_->GetChunkStore()->SubscribeChunkAdded(
        BIND(&TMasterConnector::OnChunkAdded, MakeWeak(this))
            .Via(ControlInvoker_));
    Bootstrap_->GetChunkStore()->SubscribeChunkRemoved(
        BIND(&TMasterConnector::OnChunkRemoved, MakeWeak(this))
            .Via(ControlInvoker_));

    Bootstrap_->GetChunkCache()->SubscribeChunkAdded(
        BIND(&TMasterConnector::OnChunkAdded, MakeWeak(this))
            .Via(ControlInvoker_));
    Bootstrap_->GetChunkCache()->SubscribeChunkRemoved(
        BIND(&TMasterConnector::OnChunkRemoved, MakeWeak(this))
            .Via(ControlInvoker_));

    TDelayedExecutor::Submit(
        BIND(&TMasterConnector::StartHeartbeats, MakeStrong(this))
            .Via(ControlInvoker_),
        RandomDuration(Config_->IncrementalHeartbeatPeriod));

    Started_ = true;
}

void TMasterConnector::ForceRegister()
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!Started_)
        return;

    ControlInvoker_->Invoke(BIND(
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

    return NodeId_ != InvalidNodeId;
}

TNodeId TMasterConnector::GetNodeId() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return NodeId_;
}

void TMasterConnector::RegisterAlert(const TError& alert)
{
    VERIFY_THREAD_AFFINITY_ANY();
    YCHECK(!alert.IsOK());

    LOG_WARNING(alert, "Static alert registered");

    TGuard<TSpinLock> guard(AlertsLock_);
    StaticAlerts_.push_back(alert);
}

std::vector<TError> TMasterConnector::GetAlerts()
{
    std::vector<TError> alerts;
    PopulateAlerts_.Fire(&alerts);

    for (const auto& alert : alerts) {
        LOG_WARNING(alert, "Dynamic alert registered");
    }

    TGuard<TSpinLock> guard(AlertsLock_);
    alerts.insert(alerts.end(), StaticAlerts_.begin(), StaticAlerts_.end());

    return alerts;
}

const TAddressMap& TMasterConnector::GetLocalAddresses() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return LocalAddresses_;
}

TNodeDescriptor TMasterConnector::GetLocalDescriptor() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    TGuard<TSpinLock> guard(LocalDescriptorLock_);
    return LocalDescriptor_;
}

void TMasterConnector::ScheduleNodeHeartbeat()
{
    TDelayedExecutor::Submit(
        BIND(&TMasterConnector::OnNodeHeartbeat, MakeStrong(this))
            .Via(HeartbeatInvoker_),
        Config_->IncrementalHeartbeatPeriod);
}

void TMasterConnector::ScheduleJobHeartbeat()
{
    TDelayedExecutor::Submit(
        BIND(&TMasterConnector::OnJobHeartbeat, MakeStrong(this))
            .Via(HeartbeatInvoker_),
        Config_->IncrementalHeartbeatPeriod);
}

void TMasterConnector::ResetAndScheduleRegister()
{
    Reset();

    TDelayedExecutor::Submit(
        BIND(&TMasterConnector::SendRegister, MakeStrong(this))
            .Via(HeartbeatInvoker_),
        Config_->IncrementalHeartbeatPeriod);
}

void TMasterConnector::OnNodeHeartbeat()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    switch (State_) {
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

    auto channel = Bootstrap_->GetMasterClient()->GetMasterChannel(NApi::EMasterChannelKind::Leader);
    TNodeTrackerServiceProxy proxy(channel);

    auto req = proxy.RegisterNode();
    *req->mutable_statistics() = ComputeStatistics();
    ToProto(req->mutable_addresses(), LocalAddresses_);
    req->Invoke().Subscribe(
        BIND(&TMasterConnector::OnRegisterResponse, MakeStrong(this))
            .Via(HeartbeatInvoker_));

    State_ = EState::Registering;

    LOG_INFO("Node register request sent to master (%v)",
        *req->mutable_statistics());
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

    auto chunkStore = Bootstrap_->GetChunkStore();
    yhash_set<EObjectType> acceptedChunkTypes;
    for (auto location : chunkStore->Locations()) {
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

        for (auto type : {EObjectType::Chunk, EObjectType::ErasureChunk, EObjectType::JournalChunk}) {
            if (location->IsChunkTypeAccepted(type)) {
                acceptedChunkTypes.insert(type);
            }
        }
    }

    for (auto type : acceptedChunkTypes) {
        result.add_accepted_chunk_types(static_cast<int>(type));
    }

    result.set_total_available_space(totalAvailableSpace);
    result.set_total_low_watermark_space(totalLowWatermarkSpace);
    result.set_total_used_space(totalUsedSpace);
    result.set_total_chunk_count(totalChunkCount);
    result.set_full(full);

    auto sessionManager = Bootstrap_->GetSessionManager();
    result.set_total_user_session_count(sessionManager->GetSessionCount(EWriteSessionType::User));
    result.set_total_replication_session_count(sessionManager->GetSessionCount(EWriteSessionType::Replication));
    result.set_total_repair_session_count(sessionManager->GetSessionCount(EWriteSessionType::Repair));

    auto slotManager = Bootstrap_->GetTabletSlotManager();
    result.set_available_tablet_slots(slotManager->GetAvailableTabletSlotCount());
    result.set_used_tablet_slots(slotManager->GetUsedTableSlotCount());

    const auto* tracker = Bootstrap_->GetMemoryUsageTracker();
    auto* protoMemory = result.mutable_memory();
    protoMemory->set_total_limit(tracker->GetTotalLimit());
    protoMemory->set_total_used(tracker->GetTotalUsed());
    for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
        auto* protoCategory = protoMemory->add_categories();
        protoCategory->set_type(static_cast<int>(category));
        auto limit = tracker->GetLimit(category);
        if (limit < std::numeric_limits<i64>::max()) {
            protoCategory->set_limit(limit);
        }
        auto used = tracker->GetUsed(category);
        protoCategory->set_used(used);
    }

    return result;
}

void TMasterConnector::OnRegisterResponse(const TNodeTrackerServiceProxy::TErrorOrRspRegisterNodePtr& rspOrError)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!rspOrError.IsOK()) {
        LOG_WARNING(rspOrError, "Error registering node");
        ResetAndScheduleRegister();
        return;
    }

    const auto& rsp = rspOrError.Value();
    NodeId_ = rsp->node_id();
    YCHECK(State_ == EState::Registering);
    State_ = EState::Registered;

    LOG_INFO("Successfully registered node at master (NodeId: %v)",
        NodeId_);

    SendFullNodeHeartbeat();
}

void TMasterConnector::SendFullNodeHeartbeat()
{
    auto channel = Bootstrap_->GetMasterClient()->GetMasterChannel(NApi::EMasterChannelKind::Leader);
    TNodeTrackerServiceProxy proxy(channel);

    auto request = proxy.FullHeartbeat()
        ->SetCodec(NCompression::ECodec::Lz4)
        ->SetTimeout(Config_->FullHeartbeatTimeout);

    YCHECK(NodeId_ != InvalidNodeId);
    request->set_node_id(NodeId_);

    *request->mutable_statistics() = ComputeStatistics();

    for (const auto& chunk : Bootstrap_->GetChunkStore()->GetChunks()) {
        *request->add_chunks() = BuildAddChunkInfo(chunk);
    }

    for (const auto& chunk : Bootstrap_->GetChunkCache()->GetChunks()) {
        if (!IsArtifactChunkId(chunk->GetId())) {
            *request->add_chunks() = BuildAddChunkInfo(chunk);
        }
    }

    AddedSinceLastSuccess_.clear();
    RemovedSinceLastSuccess_.clear();

    request->Invoke().Subscribe(
        BIND(&TMasterConnector::OnFullNodeHeartbeatResponse, MakeStrong(this))
            .Via(HeartbeatInvoker_));

    LOG_INFO("Full node heartbeat sent to master (%v)",
        request->statistics());
}

void TMasterConnector::SendIncrementalNodeHeartbeat()
{
    auto channel = Bootstrap_->GetMasterClient()->GetMasterChannel(NApi::EMasterChannelKind::Leader);
    TNodeTrackerServiceProxy proxy(channel);

    auto request = proxy.IncrementalHeartbeat()
        ->SetCodec(NCompression::ECodec::Lz4);

    YCHECK(NodeId_ != InvalidNodeId);
    request->set_node_id(NodeId_);

    *request->mutable_statistics() = ComputeStatistics();

    ToProto(request->mutable_alerts(), GetAlerts());

    ReportedAdded_.clear();
    for (auto chunk : AddedSinceLastSuccess_) {
        YCHECK(ReportedAdded_.insert(std::make_pair(chunk, chunk->GetVersion())).second);
        *request->add_added_chunks() = BuildAddChunkInfo(chunk);
    }

    ReportedRemoved_.clear();
    for (auto chunk : RemovedSinceLastSuccess_) {
        YCHECK(ReportedRemoved_.insert(chunk).second);
        *request->add_removed_chunks() = BuildRemoveChunkInfo(chunk);
    }

    auto slotManager = Bootstrap_->GetTabletSlotManager();
    for (auto slot : slotManager->Slots()) {
        auto* protoSlotInfo = request->add_tablet_slots();
        if (slot) {
            ToProto(protoSlotInfo->mutable_cell_info(), slot->GetCellDescriptor().ToInfo());
            protoSlotInfo->set_peer_state(static_cast<int>(slot->GetControlState()));
            protoSlotInfo->set_peer_id(slot->GetPeerId());
            ToProto(protoSlotInfo->mutable_prerequisite_transaction_id(), slot->GetPrerequisiteTransactionId());
        } else {
            protoSlotInfo->set_peer_state(static_cast<int>(NHydra::EPeerState::None));
        }
    }

    auto tabletSnapshots = slotManager->GetTabletSnapshots();
    for (auto snapshot : tabletSnapshots) {
        auto* protoTabletInfo = request->add_tablets();
        ToProto(protoTabletInfo->mutable_tablet_id(), snapshot->TabletId);

        auto* protoStatistics = protoTabletInfo->mutable_statistics();
        protoStatistics->set_partition_count(snapshot->Partitions.size());
        protoStatistics->set_store_count(snapshot->StoreCount);
        protoStatistics->set_store_preload_pending_count(snapshot->StorePreloadPendingCount);
        protoStatistics->set_store_preload_completed_count(snapshot->StorePreloadCompletedCount);
        protoStatistics->set_store_preload_failed_count(snapshot->StorePreloadFailedCount);

        auto* protoPerformanceCounters = protoTabletInfo->mutable_performance_counters();
        auto performanceCounters = snapshot->PerformanceCounters;
        protoPerformanceCounters->set_dynamic_memory_row_read_count(performanceCounters->DynamicMemoryRowReadCount);
        protoPerformanceCounters->set_dynamic_memory_row_lookup_count(performanceCounters->DynamicMemoryRowLookupCount);
        protoPerformanceCounters->set_dynamic_memory_row_write_count(performanceCounters->DynamicMemoryRowWriteCount);
        protoPerformanceCounters->set_dynamic_memory_row_delete_count(performanceCounters->DynamicMemoryRowDeleteCount);
        protoPerformanceCounters->set_static_chunk_row_read_count(performanceCounters->StaticChunkRowReadCount);
        protoPerformanceCounters->set_static_chunk_row_lookup_count(performanceCounters->StaticChunkRowLookupCount);
        protoPerformanceCounters->set_static_chunk_row_lookup_true_negative_count(performanceCounters->StaticChunkRowLookupTrueNegativeCount);
        protoPerformanceCounters->set_static_chunk_row_lookup_false_positive_count(performanceCounters->StaticChunkRowLookupFalsePositiveCount);
        protoPerformanceCounters->set_unmerged_row_read_count(performanceCounters->UnmergedRowReadCount);
        protoPerformanceCounters->set_merged_row_read_count(performanceCounters->MergedRowReadCount);
    }

    auto cellDirectory = Bootstrap_->GetMasterClient()->GetConnection()->GetCellDirectory();
    ToProto(request->mutable_hive_cells(), cellDirectory->GetRegisteredCells());

    request->Invoke().Subscribe(
        BIND(&TMasterConnector::OnIncrementalNodeHeartbeatResponse, MakeStrong(this))
            .Via(HeartbeatInvoker_));

    LOG_INFO("Incremental node heartbeat sent to master (%v, AddedChunks: %v, RemovedChunks: %v)",
        request->statistics(),
        request->added_chunks_size(),
        request->removed_chunks_size());
}

TChunkAddInfo TMasterConnector::BuildAddChunkInfo(IChunkPtr chunk)
{
    TChunkAddInfo result;
    ToProto(result.mutable_chunk_id(), chunk->GetId());
    result.set_cached(chunk->GetLocation()->GetType() == ELocationType::Cache);
    result.set_active(chunk->IsActive());
    result.set_sealed(chunk->GetInfo().sealed());
    return result;
}

TChunkRemoveInfo TMasterConnector::BuildRemoveChunkInfo(IChunkPtr chunk)
{
    TChunkRemoveInfo result;
    ToProto(result.mutable_chunk_id(), chunk->GetId());
    result.set_cached(chunk->GetLocation()->GetType() == ELocationType::Cache);
    return result;
}

void TMasterConnector::OnFullNodeHeartbeatResponse(const TNodeTrackerServiceProxy::TErrorOrRspFullHeartbeatPtr& rspOrError)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!rspOrError.IsOK()) {
        LOG_WARNING(rspOrError, "Error reporting full node heartbeat to master");
        if (IsRetriableError(rspOrError)) {
            ScheduleNodeHeartbeat();
        } else {
            ResetAndScheduleRegister();
        }
        return;
    }

    LOG_INFO("Successfully reported full node heartbeat to master");

    // Schedule another full heartbeat.
    if (Config_->FullHeartbeatPeriod) {
        TDelayedExecutor::Submit(
            BIND(&TMasterConnector::StartHeartbeats, MakeStrong(this))
                .Via(HeartbeatInvoker_),
            RandomDuration(*Config_->FullHeartbeatPeriod));
    }

    State_ = EState::Online;

    SendJobHeartbeat();
    ScheduleNodeHeartbeat();
}

void TMasterConnector::OnIncrementalNodeHeartbeatResponse(const TNodeTrackerServiceProxy::TErrorOrRspIncrementalHeartbeatPtr& rspOrError)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!rspOrError.IsOK()) {
        LOG_WARNING(rspOrError, "Error reporting incremental node heartbeat to master");
        if (IsRetriableError(rspOrError)) {
            ScheduleNodeHeartbeat();
        } else {
            ResetAndScheduleRegister();
        }
        return;
    }

    const auto& rsp = rspOrError.Value();

    auto rack = rsp->has_rack() ? MakeNullable(rsp->rack()) : Null;

    LOG_INFO("Successfully reported incremental node heartbeat to master (Rack: %v)",
        rack);

    {
        TGuard<TSpinLock> guard(LocalDescriptorLock_);
        LocalDescriptor_ = TNodeDescriptor(LocalAddresses_, rack);
    }

    {
        auto it = AddedSinceLastSuccess_.begin();
        while (it != AddedSinceLastSuccess_.end()) {
            auto jt = it++;
            auto chunk = *jt;
            auto kt = ReportedAdded_.find(chunk);
            if (kt != ReportedAdded_.end() && kt->second == chunk->GetVersion()) {
                AddedSinceLastSuccess_.erase(jt);
            }
        }
        ReportedAdded_.clear();
    }

    {
        auto it = RemovedSinceLastSuccess_.begin();
        while (it != RemovedSinceLastSuccess_.end()) {
            auto jt = it++;
            auto chunk = *jt;
            auto kt = ReportedRemoved_.find(chunk);
            if (kt != ReportedRemoved_.end()) {
                RemovedSinceLastSuccess_.erase(jt);
            }
        }
        ReportedRemoved_.clear();
    }

    auto slotManager = Bootstrap_->GetTabletSlotManager();
    for (const auto& info : rsp->tablet_slots_to_remove()) {
        auto cellId = FromProto<TCellId>(info.cell_id());
        YCHECK(cellId);
        auto slot = slotManager->FindSlot(cellId);
        if (!slot) {
            LOG_WARNING("Requested to remove a non-existing slot %v, ignored",
                cellId);
            continue;
        }
        slotManager->RemoveSlot(slot);
    }

    for (const auto& info : rsp->tablet_slots_to_create()) {
        auto cellId = FromProto<TCellId>(info.cell_id());
        YCHECK(cellId);
        if (slotManager->GetAvailableTabletSlotCount() == 0) {
            LOG_WARNING("Requested to start cell %v when all slots are used, ignored",
                cellId);
            continue;
        }
        if (slotManager->FindSlot(cellId)) {
            LOG_WARNING("Requested to start cell %v when this cell is already being served by the node, ignored",
                cellId);
            continue;
        }
        slotManager->CreateSlot(info);
    }

    for (const auto& info : rsp->tablet_slots_configure()) {
        auto descriptor = FromProto<TCellDescriptor>(info.cell_descriptor());
        auto slot = slotManager->FindSlot(descriptor.CellId);
        if (!slot) {
            LOG_WARNING("Requested to configure a non-existing slot %v, ignored",
                descriptor.CellId);
            continue;
        }
        slotManager->ConfigureSlot(slot, info);
    }

    auto cellDirectory = Bootstrap_->GetMasterClient()->GetConnection()->GetCellDirectory();

    for (const auto& info : rsp->hive_cells_to_unregister()) {
        auto cellId = FromProto<TCellId>(info.cell_id());
        YCHECK(cellId);
        if (cellDirectory->UnregisterCell(cellId)) {
            LOG_DEBUG("Hive cell unregistered (CellId: %v)",
                cellId);
        }
    }

    for (const auto& info : rsp->hive_cells_to_reconfigure()) {
        auto descriptor = FromProto<TCellDescriptor>(info.cell_descriptor());
        if (cellDirectory->ReconfigureCell(descriptor)) {
            LOG_DEBUG("Hive cell reconfigured (CellId: %v, ConfigVersion: %v)",
                descriptor.CellId,
                descriptor.ConfigVersion);
        }
    }

    ScheduleNodeHeartbeat();
}

void TMasterConnector::OnJobHeartbeat()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    
    SendJobHeartbeat();
}

void TMasterConnector::SendJobHeartbeat()
{
    YCHECK(NodeId_ != InvalidNodeId);
    YCHECK(State_ == EState::Online);

    auto channel = Bootstrap_->GetMasterClient()->GetMasterChannel(NApi::EMasterChannelKind::Leader);
    TJobTrackerServiceProxy proxy(channel);

    auto req = proxy.Heartbeat();

    auto jobController = Bootstrap_->GetJobController();
    jobController->PrepareHeartbeat(req.Get());

    req->Invoke().Subscribe(
        BIND(&TMasterConnector::OnJobHeartbeatResponse, MakeStrong(this))
            .Via(HeartbeatInvoker_));

    LOG_INFO("Job heartbeat sent to master (ResourceUsage: {%v})",
        FormatResourceUsage(req->resource_usage(), req->resource_limits()));
}

void TMasterConnector::OnJobHeartbeatResponse(const TJobTrackerServiceProxy::TErrorOrRspHeartbeatPtr& rspOrError)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!rspOrError.IsOK()) {
        LOG_WARNING(rspOrError, "Error reporting job heartbeat to master");
        if (IsRetriableError(rspOrError)) {
            ScheduleJobHeartbeat();
        } else {
            ResetAndScheduleRegister();
        }
        return;
    }

    LOG_INFO("Successfully reported job heartbeat to master");

    const auto& rsp = rspOrError.Value();
    auto jobController = Bootstrap_->GetJobController();
    jobController->ProcessHeartbeat(rsp.Get());

    ScheduleJobHeartbeat();
}

void TMasterConnector::Reset()
{
    if (HeartbeatContext_) {
        HeartbeatContext_->Cancel();
    }

    HeartbeatContext_ = New<TCancelableContext>();
    HeartbeatInvoker_ = HeartbeatContext_->CreateInvoker(ControlInvoker_);

    State_ = EState::Offline;
    NodeId_ = InvalidNodeId;

    ReportedAdded_.clear();
    ReportedRemoved_.clear();
    AddedSinceLastSuccess_.clear();
    RemovedSinceLastSuccess_.clear();
}

void TMasterConnector::OnChunkAdded(IChunkPtr chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (State_ == EState::Offline)
        return;

    if (IsArtifactChunkId(chunk->GetId()))
        return;

    RemovedSinceLastSuccess_.erase(chunk);
    AddedSinceLastSuccess_.insert(chunk);

    LOG_DEBUG("Chunk addition registered (ChunkId: %v, LocationId: %v)",
        chunk->GetId(),
        chunk->GetLocation()->GetId());
}

void TMasterConnector::OnChunkRemoved(IChunkPtr chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (State_ == EState::Offline)
        return;

    if (IsArtifactChunkId(chunk->GetId()))
        return;

    AddedSinceLastSuccess_.erase(chunk);
    RemovedSinceLastSuccess_.insert(chunk);

    LOG_DEBUG("Chunk removal registered (ChunkId: %v, LocationId: %v)",
        chunk->GetId(),
        chunk->GetLocation()->GetId());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
