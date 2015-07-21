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

#include <core/concurrency/delayed_executor.h>

#include <core/ytree/convert.h>

#include <ytlib/hydra/peer_channel.h>

#include <ytlib/election/config.h>

#include <ytlib/node_tracker_client/node_statistics.h>
#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/api/connection.h>
#include <ytlib/api/client.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/transaction_client/transaction_manager.h>

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
using namespace NTransactionClient;
using namespace NApi;
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
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(!Started_);

    Started_ = true;

    auto initializeCell = [&] (TCellTag cellTag) {
        MasterCellTags_.push_back(cellTag);
        YCHECK(ChunksDeltaMap_.insert(std::make_pair(cellTag, TChunksDelta())).second);
    };
    auto connection = Bootstrap_->GetMasterClient()->GetConnection();
    initializeCell(connection->GetPrimaryMasterCellTag());
    for (auto cellTag : connection->GetSecondaryMasterCellTags()) {
        initializeCell(cellTag);
    }

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
}

void TMasterConnector::ForceRegisterAtMaster()
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!Started_)
        return;

    ControlInvoker_->Invoke(
        BIND(&TMasterConnector::StartHeartbeats, MakeStrong(this)));
}

void TMasterConnector::StartHeartbeats()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Reset();

    HeartbeatInvoker_->Invoke(
        BIND(&TMasterConnector::RegisterAtMaster, MakeStrong(this)));
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

    {
        TGuard<TSpinLock> guard(AlertsLock_);
        StaticAlerts_.push_back(alert);
    }
}

std::vector<TError> TMasterConnector::GetAlerts()
{
    VERIFY_THREAD_AFFINITY_ANY();

    std::vector<TError> alerts;
    PopulateAlerts_.Fire(&alerts);

    for (const auto& alert : alerts) {
        LOG_WARNING(alert, "Dynamic alert registered");
    }

    {
        TGuard<TSpinLock> guard(AlertsLock_);
        alerts.insert(alerts.end(), StaticAlerts_.begin(), StaticAlerts_.end());
    }

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

void TMasterConnector::ScheduleNodeHeartbeat(TCellTag cellTag)
{
    TDelayedExecutor::Submit(
        BIND(&TMasterConnector::SendNodeHeartbeat, MakeStrong(this), cellTag)
            .Via(HeartbeatInvoker_),
        Config_->IncrementalHeartbeatPeriod);
}

void TMasterConnector::ScheduleJobHeartbeat()
{
    TDelayedExecutor::Submit(
        BIND(&TMasterConnector::SendJobHeartbeat, MakeStrong(this))
            .Via(HeartbeatInvoker_),
        Config_->IncrementalHeartbeatPeriod);
}

void TMasterConnector::ResetAndScheduleRegisterAtMaster()
{
    Reset();

    TDelayedExecutor::Submit(
        BIND(&TMasterConnector::RegisterAtMaster, MakeStrong(this))
            .Via(HeartbeatInvoker_),
        Config_->IncrementalHeartbeatPeriod);
}

void TMasterConnector::RegisterAtMaster()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    NTransactionClient::TTransactionStartOptions options;
    options.PingPeriod = Config_->LeaseTransactionPingPeriod;
    options.Timeout = Config_->LeaseTransactionTimeout;

    auto attributes = CreateEphemeralAttributes();
    attributes->Set("title", Format("Lease for node %v", GetDefaultAddress(LocalAddresses_)));
    options.Attributes = std::move(attributes);

    auto transactionManager = Bootstrap_->GetMasterClient()->GetTransactionManager();
    auto asyncTransaction = transactionManager->Start(ETransactionType::Master, options);
    auto transactionOrError = WaitFor(asyncTransaction);

    if (!transactionOrError.IsOK()) {
        LOG_ERROR(transactionOrError, "Error starting lease transaction at primary master");
        ResetAndScheduleRegisterAtMaster();
        return;
    }

    LeaseTransaction_ = transactionOrError.Value();
    LeaseTransaction_->SubscribeAborted(
        BIND(&TMasterConnector::OnLeaseTransactionAborted, MakeWeak(this))
            .Via(HeartbeatInvoker_));

    auto masterChannel = Bootstrap_->GetMasterClient()->GetMasterChannel(EMasterChannelKind::Leader);
    TNodeTrackerServiceProxy proxy(masterChannel);

    auto req = proxy.RegisterNode();
    *req->mutable_statistics() = ComputeStatistics();
    ToProto(req->mutable_addresses(), LocalAddresses_);
    ToProto(req->mutable_lease_transaction_id(), LeaseTransaction_->GetId());

    LOG_INFO("Node register request sent to primary master (%v)",
        *req->mutable_statistics());

    auto rspOrError = WaitFor(req->Invoke());

    if (!rspOrError.IsOK()) {
        LOG_WARNING(rspOrError, "Error registering node at primary master");
        ResetAndScheduleRegisterAtMaster();
        return;
    }

    const auto& rsp = rspOrError.Value();

    NodeId_ = rsp->node_id();
    for (auto cellTag : MasterCellTags_) {
        auto* delta = GetChunksDelta(cellTag);
        delta->State = EState::Registered;
    }

    LOG_INFO("Successfully registered at primary master (NodeId: %v)",
        NodeId_);

    for (auto cellTag : MasterCellTags_) {
        ScheduleNodeHeartbeat(cellTag);
    }

    ScheduleJobHeartbeat();
}

void TMasterConnector::OnLeaseTransactionAborted()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_WARNING("Master transaction lease aborted");

    ResetAndScheduleRegisterAtMaster();
}

TNodeStatistics TMasterConnector::ComputeStatistics()
{
    TNodeStatistics result;

    i64 totalAvailableSpace = 0;
    i64 totalLowWatermarkSpace = 0;
    i64 totalUsedSpace = 0;
    int totalStoredChunkCount = 0;
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
        totalStoredChunkCount += location->GetChunkCount();
        totalSessionCount += location->GetSessionCount();

        for (auto type : {EObjectType::Chunk, EObjectType::ErasureChunk, EObjectType::JournalChunk}) {
            if (location->IsChunkTypeAccepted(type)) {
                acceptedChunkTypes.insert(type);
            }
        }
    }

    auto chunkCache = Bootstrap_->GetChunkCache();
    int totalCachedChunkCount = chunkCache->GetChunkCount();

    for (auto type : acceptedChunkTypes) {
        result.add_accepted_chunk_types(static_cast<int>(type));
    }

    result.set_total_available_space(totalAvailableSpace);
    result.set_total_low_watermark_space(totalLowWatermarkSpace);
    result.set_total_used_space(totalUsedSpace);
    result.set_total_stored_chunk_count(totalStoredChunkCount);
    result.set_total_cached_chunk_count(totalCachedChunkCount);
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

void TMasterConnector::SendNodeHeartbeat(TCellTag cellTag)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto* delta = GetChunksDelta(cellTag);
    switch (delta->State) {
        case EState::Registered:
            SendFullNodeHeartbeat(cellTag);
            break;
        case EState::Online:
            SendIncrementalNodeHeartbeat(cellTag);
            break;
        default:
            YUNREACHABLE();
    }
}

void TMasterConnector::SendFullNodeHeartbeat(TCellTag cellTag)
{
    auto Logger = DataNodeLogger;
    Logger.AddTag("CellTag: %v", cellTag);

    auto channel = Bootstrap_->GetMasterClient()->GetMasterChannel(EMasterChannelKind::Leader, cellTag);
    TNodeTrackerServiceProxy proxy(channel);

    auto request = proxy.FullHeartbeat()
        ->SetCodec(NCompression::ECodec::Lz4)
        ->SetTimeout(Config_->FullHeartbeatTimeout);

    YCHECK(NodeId_ != InvalidNodeId);
    request->set_node_id(NodeId_);

    *request->mutable_statistics() = ComputeStatistics();

    // TODO(babenko): consider optimizing
    auto addChunkInfo = [&] (const IChunkPtr& chunk) {
        if (CellTagFromId(chunk->GetId()) == cellTag) {
            *request->add_chunks() = BuildAddChunkInfo(chunk);
        }
    };
    for (const auto& chunk : Bootstrap_->GetChunkStore()->GetChunks()) {
        addChunkInfo(chunk);
    }
    for (const auto& chunk : Bootstrap_->GetChunkCache()->GetChunks()) {
        addChunkInfo(chunk);
    }

    LOG_INFO("Full node heartbeat sent to master (%v)",
        request->statistics());

    auto rspOrError = WaitFor(request->Invoke());

    if (!rspOrError.IsOK()) {
        LOG_WARNING(rspOrError, "Error reporting full node heartbeat to master",
            cellTag);
        if (IsRetriableError(rspOrError)) {
            ScheduleNodeHeartbeat(cellTag);
        } else {
            ResetAndScheduleRegisterAtMaster();
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

    auto* delta = GetChunksDelta(cellTag);
    delta->State = EState::Online;
    YCHECK(delta->AddedSinceLastSuccess.empty());
    YCHECK(delta->RemovedSinceLastSuccess.empty());

    ScheduleNodeHeartbeat(cellTag);
}

void TMasterConnector::SendIncrementalNodeHeartbeat(TCellTag cellTag)
{
    auto Logger = DataNodeLogger;
    Logger.AddTag("CellTag: %v", cellTag);

    auto channel = Bootstrap_->GetMasterClient()->GetMasterChannel(EMasterChannelKind::Leader, cellTag);
    TNodeTrackerServiceProxy proxy(channel);

    auto request = proxy.IncrementalHeartbeat()
        ->SetCodec(NCompression::ECodec::Lz4);

    YCHECK(NodeId_ != InvalidNodeId);
    request->set_node_id(NodeId_);

    *request->mutable_statistics() = ComputeStatistics();

    ToProto(request->mutable_alerts(), GetAlerts());

    auto* delta = GetChunksDelta(cellTag);

    delta->ReportedAdded.clear();
    for (const auto& chunk : delta->AddedSinceLastSuccess) {
        YCHECK(delta->ReportedAdded.insert(std::make_pair(chunk, chunk->GetVersion())).second);
        *request->add_added_chunks() = BuildAddChunkInfo(chunk);
    }

    delta->ReportedRemoved.clear();
    for (const auto& chunk : delta->RemovedSinceLastSuccess) {
        YCHECK(delta->ReportedRemoved.insert(chunk).second);
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

    LOG_INFO("Incremental node heartbeat sent to master (%v, AddedChunks: %v, RemovedChunks: %v)",
        request->statistics(),
        request->added_chunks_size(),
        request->removed_chunks_size());

    auto rspOrError = WaitFor(request->Invoke());

    if (!rspOrError.IsOK()) {
        LOG_WARNING(rspOrError, "Error reporting incremental node heartbeat to master");
        if (IsRetriableError(rspOrError)) {
            ScheduleNodeHeartbeat(cellTag);
        } else {
            ResetAndScheduleRegisterAtMaster();
        }
        return;
    }

    const auto& rsp = rspOrError.Value();

    // NB: Gathering rack info from all masters is redundant but rather harmless.
    auto rack = rsp->has_rack() ? MakeNullable(rsp->rack()) : Null;

    LOG_INFO("Successfully reported incremental node heartbeat to master (Rack: %v)",
        rack);

    {
        TGuard<TSpinLock> guard(LocalDescriptorLock_);
        LocalDescriptor_ = TNodeDescriptor(LocalAddresses_, rack);
    }

    {
        auto it = delta->AddedSinceLastSuccess.begin();
        while (it != delta->AddedSinceLastSuccess.end()) {
            auto jt = it++;
            auto chunk = *jt;
            auto kt = delta->ReportedAdded.find(chunk);
            if (kt != delta->ReportedAdded.end() && kt->second == chunk->GetVersion()) {
                delta->AddedSinceLastSuccess.erase(jt);
            }
        }
        delta->ReportedAdded.clear();
    }

    {
        auto it = delta->RemovedSinceLastSuccess.begin();
        while (it != delta->RemovedSinceLastSuccess.end()) {
            auto jt = it++;
            auto chunk = *jt;
            auto kt = delta->ReportedRemoved.find(chunk);
            if (kt != delta->ReportedRemoved.end()) {
                delta->RemovedSinceLastSuccess.erase(jt);
            }
        }
        delta->ReportedRemoved.clear();
    }

    for (const auto& info : rsp->tablet_slots_to_remove()) {
        auto cellId = FromProto<TCellId>(info.cell_id());
        YCHECK(cellId != NullCellId);
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
        YCHECK(cellId != NullCellId);
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

    ScheduleNodeHeartbeat(cellTag);
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

void TMasterConnector::SendJobHeartbeat()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(NodeId_ != InvalidNodeId);

    auto cellTag = MasterCellTags_[JobHeartbeatCellIndex_];
    auto Logger = DataNodeLogger;
    Logger.AddTag("CellTag: %v", cellTag);

    auto* delta = GetChunksDelta(cellTag);
    if (delta->State == EState::Online) {
        auto channel = Bootstrap_->GetMasterClient()->GetMasterChannel(EMasterChannelKind::Leader, cellTag);
        TJobTrackerServiceProxy proxy(channel);

        auto req = proxy.Heartbeat();

        auto jobController = Bootstrap_->GetJobController();
        jobController->PrepareHeartbeat(req.Get());

        LOG_INFO("Job heartbeat sent to master (ResourceUsage: {%v})",
            FormatResourceUsage(req->resource_usage(), req->resource_limits()));

        auto rspOrError = WaitFor(req->Invoke());

        if (!rspOrError.IsOK()) {
            LOG_WARNING(rspOrError, "Error reporting job heartbeat to master");
            if (IsRetriableError(rspOrError)) {
                ScheduleJobHeartbeat();
            } else {
                ResetAndScheduleRegisterAtMaster();
            }
            return;
        }

        LOG_INFO("Successfully reported job heartbeat to master");

        const auto& rsp = rspOrError.Value();
        jobController->ProcessHeartbeat(rsp.Get());
    }

    if (++JobHeartbeatCellIndex_ >= MasterCellTags_.size()) {
        JobHeartbeatCellIndex_ = 0;
    }

    ScheduleJobHeartbeat();
}

void TMasterConnector::Reset()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (HeartbeatContext_) {
        HeartbeatContext_->Cancel();
    }

    HeartbeatContext_ = New<TCancelableContext>();
    HeartbeatInvoker_ = HeartbeatContext_->CreateInvoker(ControlInvoker_);

    NodeId_ = InvalidNodeId;
    JobHeartbeatCellIndex_ = 0;
    LeaseTransaction_.Reset();

    for (auto cellTag : MasterCellTags_) {
        auto* delta = GetChunksDelta(cellTag);
        delta->State = EState::Offline;
        delta->ReportedAdded.clear();
        delta->ReportedRemoved.clear();
        delta->AddedSinceLastSuccess.clear();
        delta->RemovedSinceLastSuccess.clear();
    }
}

void TMasterConnector::OnChunkAdded(IChunkPtr chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto* delta = GetChunksDelta(chunk->GetId());
    if (delta->State != EState::Online)
        return;

    delta->RemovedSinceLastSuccess.erase(chunk);
    delta->AddedSinceLastSuccess.insert(chunk);

    LOG_DEBUG("Chunk addition registered (ChunkId: %v)",
        chunk->GetId());
}

void TMasterConnector::OnChunkRemoved(IChunkPtr chunk)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto* delta = GetChunksDelta(chunk->GetId());
    if (delta->State != EState::Online)
        return;

    delta->AddedSinceLastSuccess.erase(chunk);
    delta->RemovedSinceLastSuccess.insert(chunk);

    LOG_DEBUG("Chunk removal registered (ChunkId: %v)",
        chunk->GetId());
}

TMasterConnector::TChunksDelta* TMasterConnector::GetChunksDelta(TCellTag cellTag)
{
    auto it = ChunksDeltaMap_.find(cellTag);
    YASSERT(it != ChunksDeltaMap_.end());
    return &it->second;
}

TMasterConnector::TChunksDelta* TMasterConnector::GetChunksDelta(const TObjectId& id)
{
    return GetChunksDelta(CellTagFromId(id));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
