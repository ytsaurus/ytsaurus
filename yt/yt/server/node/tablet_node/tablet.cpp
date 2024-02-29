#include "tablet.h"

#include "automaton.h"
#include "compression_dictionary_manager.h"
#include "distributed_throttler_manager.h"
#include "partition.h"
#include "sorted_chunk_store.h"
#include "sorted_dynamic_store.h"
#include "store_manager.h"
#include "structured_logger.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "tablet_profiling.h"
#include "transaction_manager.h"
#include "hunk_chunk.h"
#include "hunk_lock_manager.h"
#include "hedging_manager_registry.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/chaos_client/helpers.h>

#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/tls_cache.h>

namespace NYT::NTabletNode {

using namespace NChaosClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedThrottler;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NTabletClient::NProto;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using NProto::TMountHint;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TPreloadStatistics& TPreloadStatistics::operator+=(const TPreloadStatistics& other)
{
    PendingStoreCount += other.PendingStoreCount;
    CompletedStoreCount += other.CompletedStoreCount;
    FailedStoreCount += other.FailedStoreCount;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

void TCompressionDictionaryInfo::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, ChunkId);
}

void TCompressionDictionaryInfo::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, ChunkId);
}

////////////////////////////////////////////////////////////////////////////////

void ValidateTabletRetainedTimestamp(const TTabletSnapshotPtr& tabletSnapshot, TTimestamp timestamp)
{
    if (timestamp < tabletSnapshot->RetainedTimestamp) {
        THROW_ERROR_EXCEPTION("Timestamp %v is less than tablet %v retained timestamp %v",
            timestamp,
            tabletSnapshot->TabletId,
            tabletSnapshot->RetainedTimestamp);
    }
}

void ValidateTabletMounted(TTablet* tablet)
{
    if (tablet->GetState() != ETabletState::Mounted) {
        THROW_ERROR_EXCEPTION(
            NTabletClient::EErrorCode::TabletNotMounted,
            "Tablet %v is not in %Qlv state",
            tablet->GetId(),
            ETabletState::Mounted)
            << TErrorAttribute("tablet_id", tablet->GetId())
            << TErrorAttribute("table_path", tablet->GetTablePath())
            << TErrorAttribute("is_tablet_unmounted", tablet->GetState() == ETabletState::Unmounted);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TRuntimeTableReplicaData::Populate(TTableReplicaStatistics* statistics) const
{
    statistics->set_committed_replication_row_index(CommittedReplicationRowIndex.load());
    statistics->set_current_replication_timestamp(CurrentReplicationTimestamp.load());
}

void TRuntimeTableReplicaData::MergeFrom(const TTableReplicaStatistics& statistics)
{
    CommittedReplicationRowIndex = statistics.committed_replication_row_index();
    CurrentReplicationRowIndex = CommittedReplicationRowIndex.load();

    CurrentReplicationTimestamp = statistics.current_replication_timestamp();
}

////////////////////////////////////////////////////////////////////////////////

TRefCountedReplicationProgress::TRefCountedReplicationProgress(
    const NChaosClient::TReplicationProgress& progress)
    : TReplicationProgress(progress)
{ }

TRefCountedReplicationProgress::TRefCountedReplicationProgress(
    NChaosClient::TReplicationProgress&& progress)
    : TReplicationProgress(std::move(progress))
{ }

TRefCountedReplicationProgress& TRefCountedReplicationProgress::operator=(
    const NChaosClient::TReplicationProgress& progress)
{
    TReplicationProgress::operator=(progress);
    return *this;
}

TRefCountedReplicationProgress& TRefCountedReplicationProgress::operator=(
    NChaosClient::TReplicationProgress&& progress)
{
    TReplicationProgress::operator=(std::move(progress));
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

std::pair<TTabletSnapshot::TPartitionListIterator, TTabletSnapshot::TPartitionListIterator>
TTabletSnapshot::GetIntersectingPartitions(
    const TLegacyKey& lowerBound,
    const TLegacyKey& upperBound)
{
    auto beginIt = std::upper_bound(
        PartitionList.begin(),
        PartitionList.end(),
        lowerBound,
        [] (const TLegacyKey& key, const TPartitionSnapshotPtr& partition) {
            return key < partition->PivotKey;
        });

    if (beginIt != PartitionList.begin()) {
        --beginIt;
    }

    auto endIt = beginIt;
    while (endIt != PartitionList.end() && upperBound > (*endIt)->PivotKey) {
        ++endIt;
    }

    return std::pair(beginIt, endIt);
}

TPartitionSnapshotPtr TTabletSnapshot::FindContainingPartition(TLegacyKey key)
{
    auto it = std::upper_bound(
        PartitionList.begin(),
        PartitionList.end(),
        key,
        [] (TLegacyKey key, const TPartitionSnapshotPtr& partition) {
            return key < partition->PivotKey;
        });

    return it == PartitionList.begin() ? nullptr : *(--it);
}

std::vector<ISortedStorePtr> TTabletSnapshot::GetEdenStores()
{
    std::vector<ISortedStorePtr> stores;
    stores.reserve(Eden->Stores.size() + LockedStores.size());
    for (auto store : Eden->Stores) {
        stores.emplace_back(std::move(store));
    }
    for (const auto& weakStore : LockedStores) {
        auto store = weakStore.Lock();
        if (store) {
            stores.emplace_back(std::move(store));
        }
    }
    return stores;
}

bool TTabletSnapshot::IsPreallocatedDynamicStoreId(TDynamicStoreId storeId) const
{
    return std::find(PreallocatedDynamicStoreIds.begin(), PreallocatedDynamicStoreIds.end(), storeId) !=
        PreallocatedDynamicStoreIds.end();
}

IDynamicStorePtr TTabletSnapshot::FindDynamicStore(TDynamicStoreId storeId) const
{
    if (PhysicalSchema->IsSorted()) {
        for (const auto& store : Eden->Stores) {
            if (store->GetId() == storeId) {
                return store->AsDynamic();
            }
        }
        for (const auto& weakStore : LockedStores) {
            auto store = weakStore.Lock();
            if (store && store->GetId() == storeId) {
                return store->AsDynamic();
            }
        }
    } else {
        for (const auto& store : OrderedStores) {
            if (store->GetId() == storeId) {
                return store->AsDynamic();
            }
        }
    }
    return nullptr;
}

IDynamicStorePtr TTabletSnapshot::GetDynamicStoreOrThrow(TDynamicStoreId storeId) const
{
    auto dynamicStore = FindDynamicStore(storeId);
    if (!dynamicStore) {
        THROW_ERROR_EXCEPTION(
            NTabletClient::EErrorCode::NoSuchDynamicStore,
            "No such dynamic store %v",
            storeId)
            << TErrorAttribute("store_id", storeId)
            << TErrorAttribute("tablet_id", TabletId);

    }
    return dynamicStore;
}

TTableReplicaSnapshotPtr TTabletSnapshot::FindReplicaSnapshot(TTableReplicaId replicaId)
{
    auto it = Replicas.find(replicaId);
    return it == Replicas.end() ? nullptr : it->second;
}

void TTabletSnapshot::ValidateCellId(TCellId cellId)
{
    if (CellId != cellId) {
        THROW_ERROR_EXCEPTION("Wrong cell id: expected %v, got %v",
            CellId,
            cellId);
    }
}

void TTabletSnapshot::ValidateMountRevision(NHydra::TRevision mountRevision)
{
    if (MountRevision != mountRevision) {
        THROW_ERROR_EXCEPTION(
            NTabletClient::EErrorCode::InvalidMountRevision,
            "Invalid mount revision of tablet %v: expected %x, received %x",
            TabletId,
            MountRevision,
            mountRevision)
            << TErrorAttribute("tablet_id", TabletId);
    }
}

void TTabletSnapshot::WaitOnLocks(TTimestamp timestamp) const
{
    if (timestamp == AllCommittedTimestamp) {
        return;
    }

    if (Atomicity == EAtomicity::Full && timestamp == AsyncLastCommittedTimestamp) {
        return;
    }

    LockManager->Wait(timestamp, LockManagerEpoch);
}

////////////////////////////////////////////////////////////////////////////////

TTableReplicaInfo::TTableReplicaInfo(
    TTablet* tablet,
    TTableReplicaId id)
    : Tablet_(tablet)
    , Id_(id)
{ }

void TTableReplicaInfo::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Id_);
    Save(context, ClusterName_);
    Save(context, ReplicaPath_);
    Save(context, StartReplicationTimestamp_);
    Save(context, PreparedReplicationTransactionId_);
    Save(context, State_);
    Save(context, RuntimeData_->Mode);
    Save(context, RuntimeData_->CurrentReplicationRowIndex);
    Save(context, RuntimeData_->CurrentReplicationTimestamp);
    Save(context, RuntimeData_->PreparedReplicationRowIndex);
}

void TTableReplicaInfo::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, Id_);
    Load(context, ClusterName_);
    Load(context, ReplicaPath_);
    Load(context, StartReplicationTimestamp_);
    Load(context, PreparedReplicationTransactionId_);
    Load(context, State_);
    Load(context, RuntimeData_->Mode);
    Load(context, RuntimeData_->CurrentReplicationRowIndex);
    Load(context, RuntimeData_->CurrentReplicationTimestamp);
    Load(context, RuntimeData_->PreparedReplicationRowIndex);
}

ETableReplicaMode TTableReplicaInfo::GetMode() const
{
    return RuntimeData_->Mode;
}

void TTableReplicaInfo::SetMode(ETableReplicaMode value)
{
    RuntimeData_->Mode = value;
}

NTransactionClient::EAtomicity TTableReplicaInfo::GetAtomicity() const
{
    return RuntimeData_->Atomicity;
}

void TTableReplicaInfo::SetAtomicity(NTransactionClient::EAtomicity value)
{
    RuntimeData_->Atomicity = value;
}

bool TTableReplicaInfo::GetPreserveTimestamps() const
{
    return RuntimeData_->PreserveTimestamps;
}

void TTableReplicaInfo::SetPreserveTimestamps(bool value)
{
    RuntimeData_->PreserveTimestamps = value;
}

i64 TTableReplicaInfo::GetCurrentReplicationRowIndex() const
{
    return RuntimeData_->CurrentReplicationRowIndex;
}

void TTableReplicaInfo::SetCurrentReplicationRowIndex(i64 value)
{
    RuntimeData_->CurrentReplicationRowIndex = value;
}

TTimestamp TTableReplicaInfo::GetCurrentReplicationTimestamp() const
{
    return RuntimeData_->CurrentReplicationTimestamp;
}

void TTableReplicaInfo::SetCurrentReplicationTimestamp(TTimestamp value)
{
    RuntimeData_->CurrentReplicationTimestamp = value;
}

i64 TTableReplicaInfo::GetPreparedReplicationRowIndex() const
{
    return RuntimeData_->PreparedReplicationRowIndex;
}

void TTableReplicaInfo::SetPreparedReplicationRowIndex(i64 value)
{
    RuntimeData_->PreparedReplicationRowIndex = value;
}

i64 TTableReplicaInfo::GetCommittedReplicationRowIndex() const
{
    return RuntimeData_->CommittedReplicationRowIndex;
}

void TTableReplicaInfo::SetCommittedReplicationRowIndex(i64 value)
{
    RuntimeData_->CommittedReplicationRowIndex = value;
}

TError TTableReplicaInfo::GetError() const
{
    return RuntimeData_->Error.Load();
}

void TTableReplicaInfo::SetError(TError error)
{
    RuntimeData_->Error.Store(error);
}

TTableReplicaSnapshotPtr TTableReplicaInfo::BuildSnapshot() const
{
    auto snapshot = New<TTableReplicaSnapshot>();
    snapshot->StartReplicationTimestamp = StartReplicationTimestamp_;
    snapshot->RuntimeData = RuntimeData_;
    snapshot->Counters = Counters_;
    return snapshot;
}

void TTableReplicaInfo::PopulateStatistics(TTableReplicaStatistics* statistics) const
{
    RuntimeData_->Populate(statistics);
}

void TTableReplicaInfo::MergeFromStatistics(const TTableReplicaStatistics& statistics)
{
    RuntimeData_->MergeFrom(statistics);
}

ETableReplicaStatus TTableReplicaInfo::GetStatus() const
{
    return RuntimeData_->Status;
}

void TTableReplicaInfo::RecomputeReplicaStatus()
{
    auto totalRowCount = Tablet_->GetTotalRowCount();
    auto delayedLocklessRowCount = Tablet_->GetDelayedLocklessRowCount();

    ETableReplicaStatus newStatus;
    switch (GetMode()) {
        case ETableReplicaMode::Sync:
            if (State_ != ETableReplicaState::Enabled) {
                newStatus = ETableReplicaStatus::SyncNotWritable;
            } else if (GetCurrentReplicationRowIndex() >= totalRowCount + delayedLocklessRowCount) {
                newStatus = ETableReplicaStatus::SyncInSync;
            } else {
                newStatus = ETableReplicaStatus::SyncCatchingUp;
            }
            break;

        case ETableReplicaMode::Async:
            if (GetCurrentReplicationRowIndex() > totalRowCount) {
                newStatus = ETableReplicaStatus::AsyncNotWritable;
            } else if (GetCurrentReplicationRowIndex() >= totalRowCount + delayedLocklessRowCount) {
                newStatus = ETableReplicaStatus::AsyncInSync;
            } else {
                newStatus = ETableReplicaStatus::AsyncCatchingUp;
            }
            break;

        default:
            YT_ABORT();
    }

    RuntimeData_->Status = newStatus;
}

////////////////////////////////////////////////////////////////////////////////

void TBackupMetadata::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, CheckpointTimestamp_);
    Persist(context, LastPassedCheckpointTimestamp_);
    Persist(context, BackupMode_);
    Persist(context, BackupStage_);
    Persist(context, ClockClusterTag_);
    Persist(context, ReplicaBackupDescriptors_);
}

void TBackupMetadata::SetCheckpointTimestamp(TTimestamp timestamp)
{
    CheckpointTimestamp_ = timestamp;
}

TTimestamp TBackupMetadata::GetCheckpointTimestamp() const
{
    return CheckpointTimestamp_;
}

////////////////////////////////////////////////////////////////////////////////

TIdGenerator::TIdGenerator(TCellTag cellTag, ui64 counter, ui64 seed)
    : CellTag_(cellTag)
    , Counter_(counter)
    , Seed_(seed)
{ }

TObjectId TIdGenerator::GenerateId(EObjectType type)
{
    // Validate that the generator is initialized.
    YT_VERIFY(CellTag_);

    Seed_ = TRandomGenerator(Seed_).Generate<ui64>();
    ++Counter_;

    return TObjectId(
        Seed_,
        (CellTag_ << 16) + static_cast<int>(type),
        Counter_ & 0xffffffff,
        Counter_ >> 32);
}

TIdGenerator TIdGenerator::CreateDummy()
{
    return TIdGenerator(
        NObjectClient::TCellTag(1),
        /*counter*/ 0,
        /*seed*/ 0xabacabadabacabaULL);
}

void TIdGenerator::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, CellTag_);
    Persist(context, Counter_);
    Persist(context, Seed_);
}

void ToProto(NProto::TIdGenerator* protoIdGenerator, const TIdGenerator& idGenerator)
{
    protoIdGenerator->set_cell_tag(idGenerator.CellTag_);
    protoIdGenerator->set_counter(idGenerator.Counter_);
    protoIdGenerator->set_seed(idGenerator.Seed_);
}

void FromProto(TIdGenerator* idGenerator, const NProto::TIdGenerator& protoIdGenerator)
{
    idGenerator->CellTag_ = protoIdGenerator.cell_tag();
    idGenerator->Counter_ = protoIdGenerator.counter();
    idGenerator->Seed_ = protoIdGenerator.seed();
}

////////////////////////////////////////////////////////////////////////////////

void TSmoothMovementData::ValidateWriteToTablet() const
{
    if (Role_ == ESmoothMovementRole::Source) {
        switch (Stage_) {
            case ESmoothMovementStage::None:
            case ESmoothMovementStage::TargetAllocated:
                return;

            default:
                break;
        }
    } else if (Role_ == ESmoothMovementRole::Target) {
        switch (Stage_) {
            case ESmoothMovementStage::ServantSwitched:
                return;

            default:
                break;
        }
    } else {
        return;
    }

    THROW_ERROR_EXCEPTION("Cannot write into tablet since it is a "
        "smooth movement %lv in stage %Qlv",
        Role_,
        Stage_);
}

bool TSmoothMovementData::IsTabletStoresUpdateAllowed(bool isCommonFlush) const
{
    if (Role_ == ESmoothMovementRole::Source) {
        switch (Stage_) {
            case ESmoothMovementStage::None:
            case ESmoothMovementStage::TargetActivated:
                return true;

            case ESmoothMovementStage::WaitingForLocks:
                return isCommonFlush;

            default:
                return false;
        }
    } else if (Role_ == ESmoothMovementRole::Target) {
        return Stage_ == ESmoothMovementStage::ServantSwitched;
    } else {
        return true;
    }
}

bool TSmoothMovementData::ShouldForwardMutation() const
{
    if (Role_ == ESmoothMovementRole::Source) {
        switch (Stage_) {
            case ESmoothMovementStage::TargetActivated:
            case ESmoothMovementStage::ServantSwitchRequested:
                return true;

            default:
                return false;
        }
    } else {
        return false;
    }
}

void TSmoothMovementData::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Role_);
    Persist(context, Stage_);
    Persist(context, SiblingCellId_);
    Persist(context, SiblingMountRevision_);
    Persist(context, SiblingAvenueEndpointId_);
    Persist(context, CommonDynamicStoreIds_);
}

void TSmoothMovementData::BuildOrchidYson(TFluentMap fluent) const
{
    fluent
        .Item("role").Value(GetRole())
        .Item("stage").Value(GetStage())
        .Item("sibling_cell_id").Value(GetSiblingCellId())
        .Item("sibling_mount_revision").Value(GetSiblingMountRevision())
        .Item("sibling_avenue_endpoint_id").Value(GetSiblingAvenueEndpointId())
        .Item("common_dynamic_store_ids").Value(CommonDynamicStoreIds())
        .Item("stage_change_scheduled").Value(GetStageChangeScheduled());
}

////////////////////////////////////////////////////////////////////////////////

TTablet::TTablet(
    TTabletId tabletId,
    ITabletContext* context)
    : TObjectBase(tabletId)
    , TabletWriteManager_(CreateTabletWriteManager(this, context))
    , Context_(context)
    , LockManager_(New<TLockManager>())
    , HunkLockManager_(CreateHunkLockManager(this, Context_))
    , Logger(TabletNodeLogger.WithTag("TabletId: %v", Id_))
    , Settings_(TTableSettings::CreateNew())
{ }

TTablet::TTablet(
    TTabletId tabletId,
    TTableSettings settings,
    NHydra::TRevision mountRevision,
    TObjectId tableId,
    const TYPath& path,
    ITabletContext* context,
    TIdGenerator idGenerator,
    TObjectId schemaId,
    TTableSchemaPtr schema,
    TLegacyOwningKey pivotKey,
    TLegacyOwningKey nextPivotKey,
    EAtomicity atomicity,
    ECommitOrdering commitOrdering,
    TTableReplicaId upstreamReplicaId,
    TTimestamp retainedTimestamp,
    i64 cumulativeDataWeight)
    : TObjectBase(tabletId)
    , MountRevision_(mountRevision)
    , TableId_(tableId)
    , TablePath_(path)
    , SchemaId_(schemaId)
    , TableSchema_(std::move(schema))
    , PivotKey_(std::move(pivotKey))
    , NextPivotKey_(std::move(nextPivotKey))
    , State_(ETabletState::Mounted)
    , Atomicity_(atomicity)
    , CommitOrdering_(commitOrdering)
    , UpstreamReplicaId_(upstreamReplicaId)
    , HashTableSize_(settings.MountConfig->EnableLookupHashTable ? settings.MountConfig->MaxDynamicStoreRowCount : 0)
    , RetainedTimestamp_(retainedTimestamp)
    , TabletWriteManager_(CreateTabletWriteManager(this, context))
    , Context_(context)
    , IdGenerator_(idGenerator)
    , LockManager_(New<TLockManager>())
    , HunkLockManager_(CreateHunkLockManager(this, Context_))
    , Logger(TabletNodeLogger.WithTag("TabletId: %v", Id_))
    , Settings_(std::move(settings))
    , Eden_(std::make_unique<TPartition>(
        this,
        GenerateId(EObjectType::TabletPartition),
        EdenIndex,
        PivotKey_,
        NextPivotKey_))
    , CumulativeDataWeight_(cumulativeDataWeight)
{
    Initialize();
}

ETabletState TTablet::GetPersistentState() const
{
    switch (State_) {
        case ETabletState::UnmountFlushPending:
            return ETabletState::UnmountWaitingForLocks;
        case ETabletState::UnmountPending:
            return ETabletState::UnmountFlushing;
        case ETabletState::FreezeFlushPending:
            return ETabletState::FreezeWaitingForLocks;
        case ETabletState::FreezePending:
            return ETabletState::FreezeFlushing;
        default:
            return State_;
    }
}

const TTableSettings& TTablet::GetSettings() const
{
    return Settings_;
}

void TTablet::SetSettings(TTableSettings settings)
{
    Settings_ = std::move(settings);
}

const IStoreManagerPtr& TTablet::GetStoreManager() const
{
    return StoreManager_;
}

void TTablet::SetStoreManager(IStoreManagerPtr storeManager)
{
    StoreManager_ = std::move(storeManager);
}

const IPerTabletStructuredLoggerPtr& TTablet::GetStructuredLogger() const
{
    return StructuredLogger_;
}

void TTablet::SetStructuredLogger(IPerTabletStructuredLoggerPtr storeManager)
{
    StructuredLogger_ = std::move(storeManager);
}

void TTablet::ReconfigureStructuredLogger()
{
    if (StructuredLogger_) {
        StructuredLogger_->SetEnabled(Settings_.MountConfig->EnableStructuredLogger);
    }
}

const TLockManagerPtr& TTablet::GetLockManager() const
{
    return LockManager_;
}

TReplicationCardId TTablet::GetReplicationCardId() const
{
    return ReplicationCardIdFromUpstreamReplicaIdOrNull(UpstreamReplicaId_);
}

TObjectId TTablet::GenerateId(EObjectType type)
{
    // COMPAT(ifsmirnov)
    const auto* mutationContext = TryGetCurrentMutationContext();

    // NB: no mutation context in tests.
    if (!mutationContext ||
        mutationContext->Request().Reign >= static_cast<int>(ETabletReign::TabletIdGenerator))
    {
        return IdGenerator_.GenerateId(type);
    } else {
        return Context_->GenerateIdDeprecated(type);
    }
}

void TTablet::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, TableId_);
    Save(context, MountRevision_);
    Save(context, TablePath_);
    Save(context, MasterAvenueEndpointId_);
    Save(context, GetPersistentState());
    TNonNullableIntrusivePtrSerializer<>::Save(context, TableSchema_);
    Save(context, Atomicity_);
    Save(context, CommitOrdering_);
    Save(context, UpstreamReplicaId_);
    Save(context, HashTableSize_);
    Save(context, RuntimeData_->TotalRowCount);
    Save(context, RuntimeData_->TrimmedRowCount);
    Save(context, RuntimeData_->LastCommitTimestamp);
    Save(context, RuntimeData_->LastWriteTimestamp);
    Save(context, Replicas_);
    Save(context, RetainedTimestamp_);
    Save(context, CumulativeDataWeight_);

    TSizeSerializer::Save(context, StoreIdMap_.size());
    for (auto it : GetSortedIterators(StoreIdMap_)) {
        const auto& [storeId, store] = *it;
        Save(context, store->GetType());
        Save(context, storeId);
        store->Save(context);
    }

    TSizeSerializer::Save(context, HunkChunkMap_.size());
    for (auto it : GetSortedIterators(HunkChunkMap_)) {
        const auto& [chunkId, hunkChunk] = *it;
        Save(context, chunkId);
        hunkChunk->Save(context);
    }

    Save(context, ActiveStore_ ? ActiveStore_->GetId() : NullStoreId);

    auto savePartition = [&] (const TPartition& partition) {
        Save(context, partition.GetId());
        partition.Save(context);
    };

    savePartition(*Eden_);

    TSizeSerializer::Save(context, PartitionList_.size());
    for (const auto& partition : PartitionList_) {
        savePartition(*partition);
    }

    Save(context, *LockManager_);
    Save(context, DynamicStoreIdPool_);
    Save(context, DynamicStoreIdRequested_);
    Save(context, SchemaId_);
    TNullableIntrusivePtrSerializer<>::Save(context, RuntimeData_->ReplicationProgress.Acquire());
    Save(context, ChaosData_->ReplicationRound);
    Save(context, ChaosData_->CurrentReplicationRowIndexes.Load());
    Save(context, ChaosData_->PreparedWritePulledRowsTransactionId);
    Save(context, ChaosData_->PreparedAdvanceReplicationProgressTransactionId);
    Save(context, BackupMetadata_);
    Save(context, *TabletWriteManager_);
    Save(context, LastDiscardStoresRevision_);
    Save(context, StoresUpdatePreparedTransactionId_);
    Save(context, PreparedReplicatorTransactionIds_);
    Save(context, IdGenerator_);
    Save(context, CompressionDictionaryInfos_);
    Save(context, SmoothMovementData_);

    HunkLockManager_->Save(context);
}

void TTablet::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, TableId_);
    Load(context, MountRevision_);
    Load(context, TablePath_);
    // COMPAT(ifsmirnov)
    if (context.GetVersion() >= ETabletReign::Avenues) {
        Load(context, MasterAvenueEndpointId_);
    }
    Load(context, State_);
    TNonNullableIntrusivePtrSerializer<>::Load(context, TableSchema_);
    Load(context, Atomicity_);
    Load(context, CommitOrdering_);
    Load(context, UpstreamReplicaId_);
    Load(context, HashTableSize_);
    Load(context, RuntimeData_->TotalRowCount);
    Load(context, RuntimeData_->TrimmedRowCount);
    Load(context, RuntimeData_->LastCommitTimestamp);
    Load(context, RuntimeData_->LastWriteTimestamp);
    Load(context, Replicas_);
    Load(context, RetainedTimestamp_);
    Load(context, CumulativeDataWeight_);

    for (auto& [_, replicaInfo] : Replicas_) {
        replicaInfo.SetTablet(this);
    }

    // NB: Stores that we're about to create may request some tablet properties (e.g. column lock count)
    // during construction. Initialize() will take care of this.
    Initialize();

    int storeCount = TSizeSerializer::LoadSuspended(context);
    SERIALIZATION_DUMP_WRITE(context, "stores[%v]", storeCount);
    SERIALIZATION_DUMP_INDENT(context) {
        for (int index = 0; index < storeCount; ++index) {
            SERIALIZATION_DUMP_WRITE(context, "%v =>", index);
            SERIALIZATION_DUMP_INDENT(context) {
                auto storeType = Load<EStoreType>(context);
                auto storeId = Load<TStoreId> (context);
                auto store = Context_->CreateStore(this, storeType, storeId, nullptr);
                EmplaceOrCrash(StoreIdMap_, storeId, store);
                store->Load(context);
                if (store->IsChunk()) {
                    YT_VERIFY(store->AsChunk()->GetChunkId());
                }
            }
        }
    }

    int hunkChunkCount = TSizeSerializer::LoadSuspended(context);
    SERIALIZATION_DUMP_WRITE(context, "hunk_chunks[%v]", hunkChunkCount);
    SERIALIZATION_DUMP_INDENT(context) {
        for (int index = 0; index < hunkChunkCount; ++index) {
            auto chunkId = Load<TChunkId>(context);
            auto hunkChunk = Context_->CreateHunkChunk(this, chunkId, nullptr);
            EmplaceOrCrash(HunkChunkMap_, chunkId, hunkChunk);
            hunkChunk->Load(context);
            hunkChunk->Initialize();
            UpdateDanglingHunkChunks(hunkChunk);
        }
    }

    if (IsPhysicallyOrdered()) {
        for (const auto& [storeId, store] : StoreIdMap_) {
            auto orderedStore = store->AsOrdered();
            EmplaceOrCrash(StoreRowIndexMap_, orderedStore->GetStartingRowIndex(), orderedStore);
        }
    }

    auto activeStoreId = Load<TStoreId>(context);
    if (activeStoreId) {
        // COMPAT(ifsmirnov)
        auto loadedStore = FindStore(activeStoreId);
        if (loadedStore) {
            ActiveStore_ = loadedStore->AsDynamic();
        }
    }

    auto loadPartition = [&] (int index) -> std::unique_ptr<TPartition> {
        auto partitionId = LoadSuspended<TPartitionId>(context);
        SERIALIZATION_DUMP_WRITE(context, "%v =>", partitionId);
        SERIALIZATION_DUMP_INDENT(context) {
            auto partition = std::make_unique<TPartition>(
                this,
                partitionId,
                index);
            Load(context, *partition);
            for (const auto& store : partition->Stores()) {
                store->SetPartition(partition.get());
            }
            return partition;
        }
    };

    SERIALIZATION_DUMP_WRITE(context, "partitions");
    SERIALIZATION_DUMP_INDENT(context) {
        Eden_ = loadPartition(EdenIndex);

        int partitionCount = TSizeSerializer::LoadSuspended(context);
        for (int index = 0; index < partitionCount; ++index) {
            auto partition = loadPartition(index);
            EmplaceOrCrash(PartitionMap_, partition->GetId(), partition.get());
            PartitionList_.push_back(std::move(partition));
        }
    }

    Load(context, *LockManager_);

    Load(context, DynamicStoreIdPool_);
    Load(context, DynamicStoreIdRequested_);

    Load(context, SchemaId_);

    TRefCountedReplicationProgressPtr replicationProgress;
    TNullableIntrusivePtrSerializer<>::Load(context, replicationProgress);
    RuntimeData_->ReplicationProgress.Store(std::move(replicationProgress));
    Load(context, ChaosData_->ReplicationRound);
    ChaosData_->CurrentReplicationRowIndexes.Store(Load<THashMap<TTabletId, i64>>(context));

    Load(context, ChaosData_->PreparedWritePulledRowsTransactionId);
    Load(context, ChaosData_->PreparedAdvanceReplicationProgressTransactionId);

    Load(context, BackupMetadata_);

    // COMPAT(gritukan)
    if (context.GetVersion() >= ETabletReign::TabletWriteManager) {
        TabletWriteManager_->Load(context);
    }

    Load(context, LastDiscardStoresRevision_);
    // COMPAT(ifsmirnov)
    if (context.GetVersion() >= ETabletReign::SmoothTabletMovement) {
        Load(context, StoresUpdatePreparedTransactionId_);
    }
    Load(context, PreparedReplicatorTransactionIds_);

    // COMPAT(ifsmirnov)
    if (context.GetVersion() >= ETabletReign::TabletIdGenerator) {
        Load(context, IdGenerator_);
    } else {
        // Seems random enough.
        auto preSeed = Id_.Parts64[1] ^ TableId_.Parts64[1] ^ SchemaId_.Parts64[1] ^ RuntimeData_->LastCommitTimestamp ^ MountRevision_;
        auto seed = TRandomGenerator(preSeed).Generate<ui64>();

        IdGenerator_ = TIdGenerator(
            CellTagFromId(Id_),
            // Make first ids look like 1-1-... rather than 0-1-...
            /*counter*/ 1ull << 32,
            /*seed*/ seed);
    }

    // COMPAT(akozhikhov)
    if (context.GetVersion() >= ETabletReign::ValueDictionaryCompression ||
        (context.GetVersion() < ETabletReign::NoMountRevisionCheckInBulkInsert &&
         context.GetVersion() >= ETabletReign::ValueDictionaryCompression_23_2))
    {
        Load(context, CompressionDictionaryInfos_);
        for (auto policy : TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues()) {
            auto chunkId = CompressionDictionaryInfos_[policy].ChunkId;
            if (!chunkId) {
                continue;
            }
            auto dictionaryHunkChunk = GetHunkChunk(chunkId);
            YT_VERIFY(!dictionaryHunkChunk->GetAttachedCompressionDictionary());
            dictionaryHunkChunk->SetAttachedCompressionDictionary(true);
            UpdateDanglingHunkChunks(dictionaryHunkChunk);
        }
    }

    // COMPAT(ifsmirnov)
    if (context.GetVersion() >= ETabletReign::SmoothTabletMovement) {
        Load(context, SmoothMovementData_);
    }

    // COMPAT(aleksandra-zh)
    if (context.GetVersion() >= ETabletReign::JournalHunks) {
        HunkLockManager_->Load(context);
    }

    UpdateTabletSizeMetrics();
    UpdateOverlappingStoreCount();
    DynamicStoreCount_ = ComputeDynamicStoreCount();
}

TCallback<void(TSaveContext&)> TTablet::AsyncSave()
{
    std::vector<std::pair<TStoreId, TCallback<void(TSaveContext&)>>> capturedStores;
    for (const auto& [storeId, store] : StoreIdMap_) {
        capturedStores.emplace_back(storeId, store->AsyncSave());
    }

    auto capturedEden = Eden_->AsyncSave();

    std::vector<TCallback<void(TSaveContext&)>> capturedPartitions;
    for (const auto& partition : PartitionList_) {
        capturedPartitions.push_back(partition->AsyncSave());
    }

    auto capturedTabletWriteManager = TabletWriteManager_->AsyncSave();

    return BIND(
        [
            snapshot = BuildSnapshot(nullptr),
            capturedStores = std::move(capturedStores),
            capturedEden = std::move(capturedEden),
            capturedPartitions = std::move(capturedPartitions),
            capturedTabletWriteManager = std::move(capturedTabletWriteManager)
        ] (TSaveContext& context) mutable {
            using NYT::Save;

            // Save effective settings.
            Save(context, *snapshot->Settings.MountConfig);
            Save(context, *snapshot->Settings.StoreReaderConfig);
            Save(context, *snapshot->Settings.HunkReaderConfig);
            Save(context, *snapshot->Settings.StoreWriterConfig);
            Save(context, *snapshot->Settings.StoreWriterOptions);
            Save(context, *snapshot->Settings.HunkWriterConfig);
            Save(context, *snapshot->Settings.HunkWriterOptions);

            // Save raw settings.
            {
                const auto& providedSettings = snapshot->RawSettings.Provided;

                Save(context, ConvertToYsonString(providedSettings.MountConfigNode));
                if (providedSettings.ExtraMountConfig) {
                    Save(context, true);
                    Save(context, ConvertToYsonString(providedSettings.ExtraMountConfig));
                } else {
                    Save(context, false);
                }
                Save(context, *providedSettings.StoreReaderConfig);
                Save(context, *providedSettings.HunkReaderConfig);
                Save(context, *providedSettings.StoreWriterConfig);
                Save(context, *providedSettings.StoreWriterOptions);
                Save(context, *providedSettings.HunkWriterConfig);
                Save(context, *providedSettings.HunkWriterOptions);

                Save(context, *snapshot->RawSettings.GlobalPatch);
                Save(context, ConvertToYsonString(snapshot->RawSettings.Experiments));
            }

            Save(context, snapshot->PivotKey);
            Save(context, snapshot->NextPivotKey);

            capturedEden(context);

            for (const auto& callback : capturedPartitions) {
                callback(context);
            }

            SortBy(capturedStores, [] (const auto& pair) { return pair.first; });
            for (const auto& [storeId, callback] : capturedStores) {
                Save(context, storeId);
                callback(context);
            }

            capturedTabletWriteManager(context);
        });
}

void TTablet::AsyncLoad(TLoadContext& context)
{
    using NYT::Load;

    Load(context, *Settings_.MountConfig);
    // COMPAT(ifsmirnov)
    if (context.GetVersion() < ETabletReign::MountConfigExperiments) {
        RawSettings_.Provided.MountConfigNode = ConvertTo<IMapNodePtr>(Load<TYsonString>(context));
        if (Load<bool>(context)) {
            RawSettings_.Provided.ExtraMountConfig = ConvertTo<IMapNodePtr>(Load<TYsonString>(context));
        }
    }
    Load(context, *Settings_.StoreReaderConfig);
    Load(context, *Settings_.HunkReaderConfig);
    Load(context, *Settings_.StoreWriterConfig);
    Load(context, *Settings_.StoreWriterOptions);
    Load(context, *Settings_.HunkWriterConfig);
    Load(context, *Settings_.HunkWriterOptions);

    // COMPAT(ifsmirnov)
    if (context.GetVersion() >= ETabletReign::MountConfigExperiments) {
        auto& providedSettings = RawSettings_.Provided;

        providedSettings.MountConfigNode = ConvertTo<IMapNodePtr>(Load<TYsonString>(context));
        if (Load<bool>(context)) {
            providedSettings.ExtraMountConfig = ConvertTo<IMapNodePtr>(Load<TYsonString>(context));
        }

        RawSettings_.CreateNewProvidedConfigs();
        Load(context, *providedSettings.StoreReaderConfig);
        Load(context, *providedSettings.HunkReaderConfig);
        Load(context, *providedSettings.StoreWriterConfig);
        Load(context, *providedSettings.StoreWriterOptions);
        Load(context, *providedSettings.HunkWriterConfig);
        Load(context, *providedSettings.HunkWriterOptions);

        RawSettings_.GlobalPatch = New<TTableConfigPatch>();
        Load(context, *RawSettings_.GlobalPatch);
        RawSettings_.Experiments = ConvertTo<decltype(RawSettings_.Experiments)>(
            Load<TYsonString>(context));
    } else {
        auto& providedSettings = RawSettings_.Provided;

        // MountConfigNode and ExtraMountConfig should be already filled above.

        providedSettings.StoreReaderConfig = Settings_.StoreReaderConfig;
        providedSettings.HunkReaderConfig = Settings_.HunkReaderConfig;
        providedSettings.StoreWriterConfig = Settings_.StoreWriterConfig;
        providedSettings.StoreWriterOptions = Settings_.StoreWriterOptions;
        providedSettings.HunkWriterConfig = Settings_.HunkWriterConfig;
        providedSettings.HunkWriterOptions = Settings_.HunkWriterOptions;

        RawSettings_.GlobalPatch = New<TTableConfigPatch>();
    }

    Load(context, PivotKey_);
    Load(context, NextPivotKey_);

    auto loadPartition = [&] (const std::unique_ptr<TPartition>& partition) {
        SERIALIZATION_DUMP_WRITE(context, "%v =>", partition->GetId());
        SERIALIZATION_DUMP_INDENT(context) {
            partition->AsyncLoad(context);
        }
    };

    SERIALIZATION_DUMP_WRITE(context, "partitions");
    SERIALIZATION_DUMP_INDENT(context) {
        loadPartition(Eden_);
        for (const auto& partition : PartitionList_) {
            loadPartition(partition);
        }
    }

    SERIALIZATION_DUMP_WRITE(context, "stores[%v]", StoreIdMap_.size());
    SERIALIZATION_DUMP_INDENT(context) {
        for (int index = 0; index < std::ssize(StoreIdMap_); ++index) {
            auto storeId = LoadSuspended<TStoreId>(context);
            SERIALIZATION_DUMP_WRITE(context, "%v =>", storeId);
            SERIALIZATION_DUMP_INDENT(context) {
                auto store = GetStore(storeId);
                store->AsyncLoad(context);
                store->Initialize();
            }
        }
    }

    // COMPAT(gritukan)
    if (context.GetVersion() >= ETabletReign::TabletWriteManager) {
        TabletWriteManager_->AsyncLoad(context);
    }
}

void TTablet::Clear()
{
    TabletWriteManager_->Clear();
}

void TTablet::OnAfterSnapshotLoaded()
{
    TabletWriteManager_->OnAfterSnapshotLoaded();

    for (auto& dictionaryInfo : CompressionDictionaryInfos_) {
        auto chunkId = dictionaryInfo.ChunkId;
        if (!chunkId) {
            continue;
        }
        auto dictionaryHunkChunk = GetHunkChunk(chunkId);
        YT_VERIFY(dictionaryHunkChunk->GetCreationTime() != TInstant::Zero());
        dictionaryInfo.RebuildBackoffTime = dictionaryHunkChunk->GetCreationTime() +
            Settings_.MountConfig->ValueDictionaryCompression->RebuildPeriod;
    }
}

const std::vector<std::unique_ptr<TPartition>>& TTablet::PartitionList() const
{
    YT_VERIFY(IsPhysicallySorted());
    return PartitionList_;
}

TPartition* TTablet::GetEden() const
{
    YT_VERIFY(IsPhysicallySorted());
    return Eden_.get();
}

void TTablet::CreateInitialPartition()
{
    YT_VERIFY(IsPhysicallySorted());
    YT_VERIFY(PartitionList_.empty());
    auto partition = std::make_unique<TPartition>(
        this,
        GenerateId(EObjectType::TabletPartition),
        static_cast<int>(PartitionList_.size()),
        PivotKey_,
        NextPivotKey_);
    EmplaceOrCrash(PartitionMap_, partition->GetId(), partition.get());
    PartitionList_.push_back(std::move(partition));
}

TPartition* TTablet::FindPartition(TPartitionId partitionId)
{
    YT_VERIFY(IsPhysicallySorted());
    const auto& it = PartitionMap_.find(partitionId);
    return it == PartitionMap_.end() ? nullptr : it->second;
}

TPartition* TTablet::GetPartition(TPartitionId partitionId)
{
    YT_VERIFY(IsPhysicallySorted());
    auto* partition = FindPartition(partitionId);
    YT_VERIFY(partition);
    return partition;
}

void TTablet::MergePartitions(int firstIndex, int lastIndex, TDuration splitDelay)
{
    YT_VERIFY(IsPhysicallySorted());

    for (int i = lastIndex + 1; i < static_cast<int>(PartitionList_.size()); ++i) {
        PartitionList_[i]->SetIndex(i - (lastIndex - firstIndex));
    }

    auto mergedPartition = std::make_unique<TPartition>(
        this,
        GenerateId(EObjectType::TabletPartition),
        firstIndex,
        PartitionList_[firstIndex]->GetPivotKey(),
        PartitionList_[lastIndex]->GetNextPivotKey());

    mergedPartition->SetAllowedSplitTime(TInstant::Now() + splitDelay);

    std::vector<TLegacyKey> mergedSampleKeys;
    auto rowBuffer = New<TRowBuffer>(TSampleKeyListTag());

    std::vector<TLegacyOwningKey> immediateSplitKeys;
    int immediateSplitKeyCount = 0;
    for (int index = firstIndex; index <= lastIndex; ++index) {
        immediateSplitKeyCount += PartitionList_[index]->PivotKeysForImmediateSplit().size();
    }
    immediateSplitKeys.reserve(immediateSplitKeyCount);

    std::vector<TPartitionId> existingPartitionIds;

    for (int index = firstIndex; index <= lastIndex; ++index) {
        const auto& existingPartition = PartitionList_[index];
        existingPartitionIds.push_back(existingPartition->GetId());
        const auto& existingSampleKeys = existingPartition->GetSampleKeys()->Keys;
        if (index > firstIndex) {
            mergedSampleKeys.push_back(rowBuffer->CaptureRow(existingPartition->GetPivotKey()));
        }
        for (auto key : existingSampleKeys) {
            mergedSampleKeys.push_back(rowBuffer->CaptureRow(key));
        }

        for (auto& key : existingPartition->PivotKeysForImmediateSplit()) {
            immediateSplitKeys.push_back(std::move(key));
        }

        for (const auto& store : existingPartition->Stores()) {
            YT_VERIFY(store->GetPartition() == existingPartition.get());
            store->SetPartition(mergedPartition.get());
            InsertOrCrash(mergedPartition->Stores(), store);
        }
    }

    mergedPartition->GetSampleKeys()->Keys = MakeSharedRange(std::move(mergedSampleKeys), std::move(rowBuffer));

    if (!immediateSplitKeys.empty()) {
        YT_VERIFY(immediateSplitKeys[0] == mergedPartition->GetPivotKey());
        mergedPartition->RequestImmediateSplit(std::move(immediateSplitKeys));
    }

    auto firstPartitionIt = PartitionList_.begin() + firstIndex;
    auto lastPartitionIt = PartitionList_.begin() + lastIndex;
    for (auto it = firstPartitionIt; it != lastPartitionIt + 1; ++it) {
        PartitionMap_.erase((*it)->GetId());
    }
    EmplaceOrCrash(PartitionMap_, mergedPartition->GetId(), mergedPartition.get());
    PartitionList_.erase(firstPartitionIt, lastPartitionIt + 1);
    PartitionList_.insert(firstPartitionIt, std::move(mergedPartition));

    StructuredLogger_->OnPartitionsMerged(
        existingPartitionIds,
        PartitionList_[firstIndex].get());

    UpdateTabletSizeMetrics();
    UpdateOverlappingStoreCount();
}

void TTablet::SplitPartition(int index, const std::vector<TLegacyOwningKey>& pivotKeys, TDuration mergeDelay)
{
    YT_VERIFY(IsPhysicallySorted());

    auto existingPartition = std::move(PartitionList_[index]);
    YT_VERIFY(existingPartition->GetPivotKey() == pivotKeys[0]);

    for (int partitionIndex = index + 1; partitionIndex < std::ssize(PartitionList_); ++partitionIndex) {
        PartitionList_[partitionIndex]->SetIndex(partitionIndex + pivotKeys.size() - 1);
    }

    std::vector<std::unique_ptr<TPartition>> splitPartitions;
    const auto& existingSampleKeys = existingPartition->GetSampleKeys()->Keys;
    auto& existingImmediateSplitKeys = existingPartition->PivotKeysForImmediateSplit();
    int sampleKeyIndex = 0;
    int immediateSplitKeyIndex = 0;
    for (int pivotKeyIndex = 0; pivotKeyIndex < std::ssize(pivotKeys); ++pivotKeyIndex) {
        auto thisPivotKey = pivotKeys[pivotKeyIndex];
        auto nextPivotKey = (pivotKeyIndex == std::ssize(pivotKeys) - 1)
            ? existingPartition->GetNextPivotKey()
            : pivotKeys[pivotKeyIndex + 1];
        auto partition = std::make_unique<TPartition>(
            this,
            GenerateId(EObjectType::TabletPartition),
            index + pivotKeyIndex,
            thisPivotKey,
            nextPivotKey);

        if (sampleKeyIndex < std::ssize(existingSampleKeys) && existingSampleKeys[sampleKeyIndex] == thisPivotKey) {
            ++sampleKeyIndex;
        }

        YT_VERIFY(sampleKeyIndex >= std::ssize(existingSampleKeys) || existingSampleKeys[sampleKeyIndex] > thisPivotKey);

        std::vector<TLegacyKey> sampleKeys;
        auto rowBuffer = New<TRowBuffer>(TSampleKeyListTag());

        while (sampleKeyIndex < std::ssize(existingSampleKeys) && existingSampleKeys[sampleKeyIndex] < nextPivotKey) {
            sampleKeys.push_back(rowBuffer->CaptureRow(existingSampleKeys[sampleKeyIndex]));
            ++sampleKeyIndex;
        }

        partition->GetSampleKeys()->Keys = MakeSharedRange(std::move(sampleKeys), std::move(rowBuffer));

        if (existingPartition->IsImmediateSplitRequested() && immediateSplitKeyIndex != std::ssize(existingImmediateSplitKeys)) {
            if (existingImmediateSplitKeys[immediateSplitKeyIndex] == thisPivotKey) {
                ++immediateSplitKeyIndex;
            }

            int lastKeyIndex = immediateSplitKeyIndex;
            while (lastKeyIndex < std::ssize(existingImmediateSplitKeys) && existingImmediateSplitKeys[lastKeyIndex] < nextPivotKey) {
                ++lastKeyIndex;
            }

            if (lastKeyIndex != immediateSplitKeyIndex) {
                std::vector<TLegacyOwningKey> immediateSplitKeys;
                immediateSplitKeys.reserve(lastKeyIndex - immediateSplitKeyIndex + 1);

                immediateSplitKeys.push_back(thisPivotKey);

                immediateSplitKeys.insert(
                    immediateSplitKeys.end(),
                    std::make_move_iterator(existingImmediateSplitKeys.begin()) + immediateSplitKeyIndex,
                    std::make_move_iterator(existingImmediateSplitKeys.begin()) + lastKeyIndex);
                immediateSplitKeyIndex = lastKeyIndex;

                partition->RequestImmediateSplit(std::move(immediateSplitKeys));
            }
        }

        partition->SetAllowedMergeTime(TInstant::Now() + mergeDelay);

        splitPartitions.push_back(std::move(partition));
    }

    PartitionMap_.erase(existingPartition->GetId());
    for (const auto& partition : splitPartitions) {
        EmplaceOrCrash(PartitionMap_, partition->GetId(), partition.get());
    }
    PartitionList_.erase(PartitionList_.begin() + index);
    PartitionList_.insert(
        PartitionList_.begin() + index,
        std::make_move_iterator(splitPartitions.begin()),
        std::make_move_iterator(splitPartitions.end()));

    for (const auto& store : existingPartition->Stores()) {
        YT_VERIFY(store->GetPartition() == existingPartition.get());
        auto* newPartition = GetContainingPartition(store);
        store->SetPartition(newPartition);
        InsertOrCrash(newPartition->Stores(), store);
    }

    StructuredLogger_->OnPartitionSplit(
        existingPartition.get(),
        index,
        pivotKeys.size());

    UpdateTabletSizeMetrics();
    UpdateOverlappingStoreCount();
}

TPartition* TTablet::GetContainingPartition(
    const TLegacyOwningKey& minKey,
    const TLegacyOwningKey& upperBoundKey)
{
    YT_VERIFY(IsPhysicallySorted());

    auto it = std::upper_bound(
        PartitionList_.begin(),
        PartitionList_.end(),
        minKey,
        [] (const TLegacyOwningKey& key, const std::unique_ptr<TPartition>& partition) {
            return key < partition->GetPivotKey();
        });

    if (it != PartitionList_.begin()) {
        --it;
    }

    if (it + 1 == PartitionList().end()) {
        return it->get();
    }

    if ((*(it + 1))->GetPivotKey() >= upperBoundKey) {
        return it->get();
    }

    return Eden_.get();
}

const THashMap<TStoreId, IStorePtr>& TTablet::StoreIdMap() const
{
    return StoreIdMap_;
}

const std::map<i64, IOrderedStorePtr>& TTablet::StoreRowIndexMap() const
{
    YT_VERIFY(IsPhysicallyOrdered());
    return StoreRowIndexMap_;
}

void TTablet::AddStore(IStorePtr store, bool onFlush, TPartitionId partitionIdHint)
{
    EmplaceOrCrash(StoreIdMap_, store->GetId(), store);
    if (IsPhysicallySorted()) {
        auto sortedStore = store->AsSorted();

        auto* partition = partitionIdHint
            ? Eden_->GetId() == partitionIdHint
                ? Eden_.get()
                : FindPartition(partitionIdHint)
            : onFlush && Settings_.MountConfig->AlwaysFlushToEden
                ? GetEden()
                : GetContainingPartition(sortedStore);
        YT_VERIFY(partition);
        InsertOrCrash(partition->Stores(), sortedStore);
        sortedStore->SetPartition(partition);
        UpdateOverlappingStoreCount();

        if (store->GetStoreState() != EStoreState::ActiveDynamic) {
            NonActiveStoresUnmergedRowCount_ += store->GetRowCount();
        }
    } else {
        auto orderedStore = store->AsOrdered();
        EmplaceOrCrash(StoreRowIndexMap_, orderedStore->GetStartingRowIndex(), orderedStore);
    }

    if (store->IsDynamic()) {
        ++DynamicStoreCount_;
    }

    UpdateTabletSizeMetrics();
}

void TTablet::RemoveStore(IStorePtr store)
{
    if (store->IsDynamic()) {
        --DynamicStoreCount_;
    }

    EraseOrCrash(StoreIdMap_, store->GetId());
    if (IsPhysicallySorted()) {
        auto sortedStore = store->AsSorted();
        auto* partition = sortedStore->GetPartition();
        EraseOrCrash(partition->Stores(), sortedStore);
        sortedStore->SetPartition(nullptr);
        UpdateOverlappingStoreCount();

        if (store->GetStoreState() != EStoreState::ActiveDynamic) {
            YT_VERIFY(NonActiveStoresUnmergedRowCount_ >= store->GetRowCount());
            NonActiveStoresUnmergedRowCount_ -= store->GetRowCount();
        }
    } else {
        auto orderedStore = store->AsOrdered();
        EraseOrCrash(StoreRowIndexMap_, orderedStore->GetStartingRowIndex());
    }

    UpdateTabletSizeMetrics();
}

IStorePtr TTablet::FindStore(TStoreId id)
{
    auto it = StoreIdMap_.find(id);
    return it == StoreIdMap_.end() ? nullptr : it->second;
}

IStorePtr TTablet::GetStore(TStoreId id)
{
    auto store = FindStore(id);
    YT_VERIFY(store);
    return store;
}

IStorePtr TTablet::GetStoreOrThrow(TStoreId id)
{
    auto store = FindStore(id);
    if (!store) {
        THROW_ERROR_EXCEPTION("No such store %v", id);
    }
    return store;
}

const THashMap<TChunkId, THunkChunkPtr>& TTablet::HunkChunkMap() const
{
    return HunkChunkMap_;
}

void TTablet::AddHunkChunk(THunkChunkPtr hunkChunk)
{
    EmplaceOrCrash(HunkChunkMap_, hunkChunk->GetId(), hunkChunk);
}

void TTablet::RemoveHunkChunk(THunkChunkPtr hunkChunk)
{
    EraseOrCrash(HunkChunkMap_, hunkChunk->GetId());
    // NB: May be missing.
    DanglingHunkChunks_.erase(hunkChunk);
}

THunkChunkPtr TTablet::FindHunkChunk(TChunkId id)
{
    auto it = HunkChunkMap_.find(id);
    return it == HunkChunkMap_.end() ? nullptr : it->second;
}

THunkChunkPtr TTablet::GetHunkChunk(TChunkId id)
{
    auto hunkChunk = FindHunkChunk(id);
    YT_VERIFY(hunkChunk);
    return hunkChunk;
}

THunkChunkPtr TTablet::GetHunkChunkOrThrow(TChunkId id)
{
    auto hunkChunk = FindHunkChunk(id);
    if (!hunkChunk) {
        THROW_ERROR_EXCEPTION("No such hunk chunk %v", id);
    }
    return hunkChunk;
}

void TTablet::AttachCompressionDictionary(
    NTableClient::EDictionaryCompressionPolicy policy,
    NChunkClient::TChunkId chunkId)
{
    auto oldDictionaryId = CompressionDictionaryInfos_[policy].ChunkId;
    CompressionDictionaryInfos_[policy].ChunkId = chunkId;

    if (oldDictionaryId) {
        auto oldDictionaryHunkChunk = GetHunkChunk(oldDictionaryId);
        YT_VERIFY(oldDictionaryHunkChunk->GetAttachedCompressionDictionary());
        oldDictionaryHunkChunk->SetAttachedCompressionDictionary(false);
        UpdateDanglingHunkChunks(oldDictionaryHunkChunk);
    }

    if (chunkId) {
        auto dictionaryHunkChunk = GetHunkChunk(chunkId);
        YT_VERIFY(!dictionaryHunkChunk->GetAttachedCompressionDictionary());
        dictionaryHunkChunk->SetAttachedCompressionDictionary(true);
        UpdateDanglingHunkChunks(dictionaryHunkChunk);
    }
}

void TTablet::UpdatePreparedStoreRefCount(const THunkChunkPtr& hunkChunk, int delta)
{
    hunkChunk->SetPreparedStoreRefCount(hunkChunk->GetPreparedStoreRefCount() + delta);
    UpdateDanglingHunkChunks(hunkChunk);
}

void TTablet::UpdateHunkChunkRef(const THunkChunkRef& ref, int delta)
{
    const auto& hunkChunk = ref.HunkChunk;
    hunkChunk->SetReferencedHunkCount(hunkChunk->GetReferencedHunkCount() + ref.HunkCount * delta);
    hunkChunk->SetReferencedTotalHunkLength(hunkChunk->GetReferencedTotalHunkLength() + ref.TotalHunkLength * delta);
    hunkChunk->SetStoreRefCount(hunkChunk->GetStoreRefCount() + delta);
    UpdateDanglingHunkChunks(hunkChunk);
}

const THashSet<THunkChunkPtr>& TTablet::DanglingHunkChunks() const
{
    return DanglingHunkChunks_;
}

TTableReplicaInfo* TTablet::FindReplicaInfo(TTableReplicaId id)
{
    auto it = Replicas_.find(id);
    return it == Replicas_.end() ? nullptr : &it->second;
}

TTableReplicaInfo* TTablet::GetReplicaInfoOrThrow(TTableReplicaId id)
{
    auto* info = FindReplicaInfo(id);
    if (!info) {
        THROW_ERROR_EXCEPTION("No such replica %v", id);
    }
    return info;
}

bool TTablet::IsPhysicallySorted() const
{
    return PhysicalSchema_->GetKeyColumnCount() > 0;
}

bool TTablet::IsPhysicallyOrdered() const
{
    return PhysicalSchema_->GetKeyColumnCount() == 0;
}

bool TTablet::IsReplicated() const
{
    return TypeFromId(TableId_) == EObjectType::ReplicatedTable;
}

bool TTablet::IsPhysicallyLog() const
{
    return IsLogTableType(TypeFromId(TableId_));
}

int TTablet::GetColumnLockCount() const
{
    return LockIndexToName_.size();
}

i64 TTablet::GetTotalRowCount() const
{
    return RuntimeData_->TotalRowCount.load();
}

void TTablet::UpdateTotalRowCount()
{
    if (StoreRowIndexMap_.empty()) {
        RuntimeData_->TotalRowCount = 0;
    } else {
        auto it = StoreRowIndexMap_.rbegin();
        RuntimeData_->TotalRowCount = it->first + it->second->GetRowCount();
    }
}

i64 TTablet::GetDelayedLocklessRowCount()
{
    return RuntimeData_->DelayedLocklessRowCount;
}

void TTablet::SetDelayedLocklessRowCount(i64 value)
{
    RuntimeData_->DelayedLocklessRowCount = value;
}

i64 TTablet::GetTrimmedRowCount() const
{
    return RuntimeData_->TrimmedRowCount;
}

void TTablet::SetTrimmedRowCount(i64 value)
{
    RuntimeData_->TrimmedRowCount = value;
}

i64 TTablet::GetCumulativeDataWeight() const
{
    return CumulativeDataWeight_;
}

void TTablet::IncreaseCumulativeDataWeight(i64 delta)
{
    YT_ASSERT(delta >= 0);
    CumulativeDataWeight_ += delta;
}

TTimestamp TTablet::GetLastCommitTimestamp() const
{
    return RuntimeData_->LastCommitTimestamp;
}

void TTablet::UpdateLastCommitTimestamp(TTimestamp value)
{
    RuntimeData_->LastCommitTimestamp = std::max(value, GetLastCommitTimestamp());
    RuntimeData_->LastWriteTimestamp = std::max(value, GetLastWriteTimestamp());
}

TTimestamp TTablet::GetLastWriteTimestamp() const
{
    return RuntimeData_->LastWriteTimestamp;
}

void TTablet::UpdateLastWriteTimestamp(TTimestamp value)
{
    RuntimeData_->LastWriteTimestamp = std::max(value, GetLastWriteTimestamp());
}

TTimestamp TTablet::GetUnflushedTimestamp() const
{
    return RuntimeData_->UnflushedTimestamp;
}

void TTablet::Reconfigure(const ITabletSlotPtr& slot)
{
    ReconfigureLocalThrottlers();
    ReconfigureDistributedThrottlers(slot);
    ReconfigureChunkFragmentReader(slot);
    ReconfigureProfiling();
    ReconfigureStructuredLogger();
    ReconfigureRowCache(slot);
    InvalidateChunkReaders();
    ReconfigureHedgingManagerRegistry();
    ReconfigureCompressionDictionaries();
}

void TTablet::StartEpoch(const ITabletSlotPtr& slot)
{
    CancelableContext_ = New<TCancelableContext>();

    for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
        EpochAutomatonInvokers_[queue] = CancelableContext_->CreateInvoker(
            // NB: Slot can be null in tests.
            slot
            ? slot->GetEpochAutomatonInvoker(queue)
            : GetCurrentInvoker());
    }

    Eden_->StartEpoch();
    for (const auto& partition : PartitionList_) {
        partition->StartEpoch();
    }

    ReconfigureRowCache(slot);

    HunkLockManager_->StartEpoch();
    TabletWriteManager_->StartEpoch();
}

void TTablet::StopEpoch()
{
    if (CancelableContext_) {
        CancelableContext_->Cancel(TError("Tablet epoch canceled"));
        CancelableContext_.Reset();
    }

    std::fill(EpochAutomatonInvokers_.begin(), EpochAutomatonInvokers_.end(), GetNullInvoker());

    SetState(GetPersistentState());

    Eden_->StopEpoch();
    for (const auto& partition : PartitionList_) {
        partition->StopEpoch();
    }

    RowCache_.Reset();

    HunkLockManager_->StopEpoch();
    TabletWriteManager_->StopEpoch();
}

IInvokerPtr TTablet::GetEpochAutomatonInvoker(EAutomatonThreadQueue queue) const
{
    return EpochAutomatonInvokers_[queue];
}

TTabletSnapshotPtr TTablet::BuildSnapshot(
    const ITabletSlotPtr& slot,
    std::optional<TLockManagerEpoch> epoch) const
{
    auto snapshot = New<TTabletSnapshot>();

    if (slot) {
        snapshot->CellId = slot->GetCellId();
        snapshot->HydraManager = slot->GetHydraManager();
        snapshot->TabletCellRuntimeData = slot->GetRuntimeData();
        snapshot->DictionaryCompressionFactory = slot->GetCompressionDictionaryManager()
            ->CreateTabletDictionaryCompressionFactory(snapshot);
    }

    snapshot->TabletId = Id_;
    snapshot->LoggingTag = LoggingTag_;
    snapshot->MountRevision = MountRevision_;
    snapshot->TablePath = TablePath_;
    snapshot->TableId = TableId_;
    snapshot->Settings = Settings_;
    snapshot->RawSettings = RawSettings_;
    snapshot->PivotKey = PivotKey_;
    snapshot->NextPivotKey = NextPivotKey_;
    snapshot->TableSchema = TableSchema_;
    snapshot->SchemaId = SchemaId_;
    snapshot->PhysicalSchema = PhysicalSchema_;
    snapshot->QuerySchema = PhysicalSchema_->ToQuery();
    snapshot->TableSchemaData = TableSchemaData_;
    snapshot->KeysSchemaData = KeysSchemaData_;
    snapshot->Atomicity = Atomicity_;
    snapshot->UpstreamReplicaId = UpstreamReplicaId_;
    snapshot->HashTableSize = HashTableSize_;
    snapshot->StoreCount = static_cast<int>(StoreIdMap_.size());
    snapshot->OverlappingStoreCount = OverlappingStoreCount_;
    snapshot->EdenOverlappingStoreCount = EdenOverlappingStoreCount_;
    snapshot->CriticalPartitionCount = CriticalPartitionCount_;
    snapshot->RetainedTimestamp = RetainedTimestamp_;
    snapshot->FlushThrottler = FlushThrottler_;
    snapshot->CompactionThrottler = CompactionThrottler_;
    snapshot->PartitioningThrottler = PartitioningThrottler_;
    snapshot->LockManager = LockManager_;
    snapshot->LockManagerEpoch = epoch.value_or(LockManager_->GetEpoch());
    snapshot->RowCache = RowCache_;
    snapshot->StoreFlushIndex = StoreFlushIndex_;
    snapshot->DistributedThrottlers = DistributedThrottlers_;
    snapshot->HedgingManagerRegistry = HedgingManagerRegistry_;
    snapshot->CompressionDictionaryInfos = CompressionDictionaryInfos_;

    auto addStoreStatistics = [&] (const IStorePtr& store) {
        if (store->IsChunk()) {
            auto chunkStore = store->AsChunk();

            auto preloadState = chunkStore->GetPreloadState();
            switch (preloadState) {
                case EStorePreloadState::Scheduled:
                case EStorePreloadState::Running:
                    if (chunkStore->IsPreloadAllowed()) {
                        ++snapshot->PreloadPendingStoreCount;
                    } else {
                        ++snapshot->PreloadFailedStoreCount;
                    }
                    break;
                case EStorePreloadState::Complete:
                    ++snapshot->PreloadCompletedStoreCount;
                    break;
                default:
                    break;
            }
        }
    };

    auto addPartitionStatistics = [&] (const TPartitionSnapshotPtr& partitionSnapshot) {
        for (const auto& store : partitionSnapshot->Stores) {
            addStoreStatistics(store);
        }
    };

    snapshot->Eden = Eden_->BuildSnapshot();
    addPartitionStatistics(snapshot->Eden);
    snapshot->ActiveStore = ActiveStore_;

    snapshot->PartitionList.reserve(PartitionList_.size());
    for (const auto& partition : PartitionList_) {
        auto partitionSnapshot = partition->BuildSnapshot();
        snapshot->PartitionList.push_back(partitionSnapshot);
        addPartitionStatistics(partitionSnapshot);
    }

    if (IsPhysicallyOrdered()) {
        // TODO(babenko): optimize
        snapshot->OrderedStores.reserve(StoreRowIndexMap_.size());
        for (const auto& [_, store] : StoreRowIndexMap_) {
            snapshot->OrderedStores.push_back(store);
            addStoreStatistics(store);
        }

        snapshot->TotalRowCount = GetTotalRowCount();
    }

    if (IsPhysicallySorted() && StoreManager_) {
        auto lockedStores = StoreManager_->GetLockedStores();
        for (const auto& store : lockedStores) {
            snapshot->LockedStores.push_back(store->AsSorted());
        }
    }

    if (Settings_.MountConfig->EnableDynamicStoreRead) {
        snapshot->PreallocatedDynamicStoreIds = std::vector<TDynamicStoreId>(
            DynamicStoreIdPool_.begin(),
            DynamicStoreIdPool_.end());
    }

    snapshot->RowKeyComparer = RowKeyComparer_;
    snapshot->PerformanceCounters = PerformanceCounters_;
    snapshot->ColumnEvaluator = ColumnEvaluator_;
    snapshot->TabletRuntimeData = RuntimeData_;
    snapshot->TabletChaosData = ChaosData_;

    for (const auto& [replicaId, replicaInfo] : Replicas_) {
        EmplaceOrCrash(snapshot->Replicas, replicaId, replicaInfo.BuildSnapshot());
    }

    UpdateUnflushedTimestamp();

    snapshot->TableProfiler = TableProfiler_;

    snapshot->ConsistentChunkReplicaPlacementHash = GetConsistentChunkReplicaPlacementHash();

    snapshot->ChunkFragmentReader = GetChunkFragmentReader();

    snapshot->TabletCellBundle = Context_->GetTabletCellBundleName();

    return snapshot;
}

void TTablet::Initialize()
{
    PhysicalSchema_ = IsPhysicallyLog() ? TableSchema_->ToReplicationLog() : TableSchema_;

    TableSchemaData_ = IWireProtocolReader::GetSchemaData(*TableSchema_);
    KeysSchemaData_ = IWireProtocolReader::GetSchemaData(*PhysicalSchema_->ToKeys());

    RowKeyComparer_ = Context_->GetRowComparerProvider()->Get(PhysicalSchema_->GetKeyColumnTypes());

    auto groupToIndex = GetLocksMapping(
        *PhysicalSchema_,
        Atomicity_ == EAtomicity::Full,
        &ColumnIndexToLockIndex_,
        &LockIndexToName_);

    ColumnEvaluator_ = Context_->GetColumnEvaluatorCache()->Find(PhysicalSchema_);

    StoresUpdateCommitSemaphore_ = New<NConcurrency::TAsyncSemaphore>(1);

    FlushThrottler_ = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(),
        Logger);
    CompactionThrottler_ = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(),
        Logger);
    PartitioningThrottler_ = CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(),
        Logger);

    LoggingTag_ = Format("TabletId: %v, TableId: %v, TablePath: %v",
        Id_,
        TableId_,
        TablePath_);

    HunkLockManager_->Initialize();
}

void TTablet::ReconfigureRowCache(const ITabletSlotPtr& slot)
{
    if (!slot || !slot->GetHydraManager()->IsLeader()) {
        return;
    }

    i64 lookupCacheCapacity = Settings_.MountConfig->LookupCacheRowsPerTablet;
    double lookupCacheRowsRatio = Settings_.MountConfig->LookupCacheRowsRatio;

    if (lookupCacheRowsRatio > 0) {
        i64 unmergedRowCount = NonActiveStoresUnmergedRowCount_;
        if (ActiveStore_) {
            unmergedRowCount += ActiveStore_->GetRowCount();
        }

        lookupCacheCapacity = lookupCacheRowsRatio * unmergedRowCount;
    }

    if (lookupCacheCapacity == 0) {
        RowCache_.Reset();
        return;
    }

    if (!RowCache_) {
        RowCache_ = New<TRowCache>(
            lookupCacheCapacity,
            TabletNodeProfiler.WithTag("table_path", TablePath_).WithPrefix("/row_cache"),
            Context_
                ->GetMemoryUsageTracker()
                ->WithCategory(EMemoryCategory::LookupRowsCache));
    }

    RowCache_->GetCache()->SetCapacity(lookupCacheCapacity);
}

void TTablet::InvalidateChunkReaders()
{
    for (const auto& [_, store] : StoreIdMap_) {
        if (store->IsChunk()) {
            store->AsChunk()->InvalidateCachedReaders(Settings_);
        }
    }
}

void TTablet::ReconfigureHedgingManagerRegistry()
{
    HedgingManagerRegistry_ = Context_->GetHedgingManagerRegistry()->GetOrCreateTabletHedgingManagerRegistry(
        TableId_,
        Settings_.StoreReaderConfig->HedgingManager,
        Settings_.HunkReaderConfig->HedgingManager,
        TableProfiler_->GetProfiler());
}

void TTablet::ReconfigureCompressionDictionaries()
{
    if (Settings_.MountConfig->ValueDictionaryCompression->Enable) {
        for (auto policy : TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues()) {
            if (CompressionDictionaryInfos_[policy].ChunkId &&
                !Settings_.MountConfig->ValueDictionaryCompression->AppliedPolicies.contains(policy))
            {
                AttachCompressionDictionary(policy, NullChunkId);
                CompressionDictionaryInfos_[policy].RebuildBackoffTime = TInstant::Zero();
            }
        }
    } else {
        for (auto policy : TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues()) {
            AttachCompressionDictionary(policy, NullChunkId);
            CompressionDictionaryInfos_[policy].RebuildBackoffTime = TInstant::Zero();
        }
    }
}

void TTablet::ReconfigureProfiling()
{
    TableProfiler_ = CreateTableProfiler(
        Settings_.MountConfig->ProfilingMode,
        Context_->GetTabletCellBundleName(),
        TablePath_,
        Settings_.MountConfig->ProfilingTag,
        // TODO(babenko): hunks profiling
        Settings_.StoreWriterOptions->Account,
        Settings_.StoreWriterOptions->MediumName,
        SchemaId_,
        PhysicalSchema_);
    TabletCounters_ = TableProfiler_->GetTabletCounters();

    TabletCounters_.TabletCount.Update(1);

    UpdateReplicaCounters();
}

void TTablet::ReconfigureLocalThrottlers()
{
    FlushThrottler_->Reconfigure(Settings_.MountConfig->FlushThrottler);
    CompactionThrottler_->Reconfigure(Settings_.MountConfig->CompactionThrottler);
    PartitioningThrottler_->Reconfigure(Settings_.MountConfig->PartitioningThrottler);
}

void TTablet::ReconfigureDistributedThrottlers(const ITabletSlotPtr& slot)
{
    if (!slot) {
        return;
    }

    const auto& throttlerManager = slot->GetDistributedThrottlerManager();

    auto getThrottlerConfig = [&] (const TString& key) {
        auto it = Settings_.MountConfig->Throttlers.find(key);
        return it != Settings_.MountConfig->Throttlers.end()
            ? it->second
            : New<TThroughputThrottlerConfig>();
    };

    DistributedThrottlers_[ETabletDistributedThrottlerKind::StoresUpdate] =
        throttlerManager->GetOrCreateThrottler(
            TablePath_,
            CellTagFromId(Id_),
            getThrottlerConfig("tablet_stores_update"),
            "tablet_stores_update",
            EDistributedThrottlerMode::Precise,
            TabletStoresUpdateThrottlerRpcTimeout,
            /*admitUnlimitedThrottler*/ true);

    DistributedThrottlers_[ETabletDistributedThrottlerKind::Lookup] =
        throttlerManager->GetOrCreateThrottler(
            TablePath_,
            CellTagFromId(Id_),
            getThrottlerConfig("lookup"),
            "lookup",
            EDistributedThrottlerMode::Adaptive,
            LookupThrottlerRpcTimeout,
            /*admitUnlimitedThrottler*/ false);
    YT_VERIFY(
        Settings_.MountConfig->Throttlers.contains("lookup") ||
        !DistributedThrottlers_[ETabletDistributedThrottlerKind::Lookup]);

    DistributedThrottlers_[ETabletDistributedThrottlerKind::Select] =
        throttlerManager->GetOrCreateThrottler(
            TablePath_,
            CellTagFromId(Id_),
            getThrottlerConfig("select"),
            "select",
            EDistributedThrottlerMode::Adaptive,
            SelectThrottlerRpcTimeout,
            /*admitUnlimitedThrottler*/ false);

    DistributedThrottlers_[ETabletDistributedThrottlerKind::CompactionRead] =
        throttlerManager->GetOrCreateThrottler(
            TablePath_,
            CellTagFromId(Id_),
            getThrottlerConfig("compaction_read"),
            "compaction_read",
            EDistributedThrottlerMode::Adaptive,
            CompactionReadThrottlerRpcTimeout,
            /*admitUnlimitedThrottler*/ false);

    DistributedThrottlers_[ETabletDistributedThrottlerKind::Write] =
        throttlerManager->GetOrCreateThrottler(
            TablePath_,
            CellTagFromId(Id_),
            getThrottlerConfig("write"),
            "write",
            EDistributedThrottlerMode::Adaptive,
            WriteThrottlerRpcTimeout,
            /*admitUnlimitedThrottler*/ false);

    DistributedThrottlers_[ETabletDistributedThrottlerKind::ChangelogMediumWrite] =
        slot->GetChangelogMediumWriteThrottler();
    DistributedThrottlers_[ETabletDistributedThrottlerKind::BlobMediumWrite] =
        slot->GetMediumWriteThrottler(Settings_.StoreWriterOptions->MediumName);

    YT_VERIFY(DistributedThrottlers_[ETabletDistributedThrottlerKind::ChangelogMediumWrite]);
    YT_VERIFY(DistributedThrottlers_[ETabletDistributedThrottlerKind::BlobMediumWrite]);
}

void TTablet::ReconfigureChunkFragmentReader(const ITabletSlotPtr& slot)
{
    if (!slot) {
        return;
    }

    ChunkFragmentReader_ = slot->CreateChunkFragmentReader(this);
}

const TString& TTablet::GetLoggingTag() const
{
    return LoggingTag_;
}

std::optional<TString> TTablet::GetPoolTagByMemoryCategory(EMemoryCategory category) const
{
    if (category == EMemoryCategory::TabletDynamic) {
        return Context_->GetTabletCellBundleName();
    }
    return {};
}

void TTablet::UpdateReplicaCounters()
{
    for (auto& [replicaId, replica] : Replicas_) {
        replica.SetCounters(TableProfiler_->GetReplicaCounters(replica.GetClusterName()));
    }
}

TPartition* TTablet::GetContainingPartition(const ISortedStorePtr& store)
{
    // Dynamic stores must reside in Eden.
    if (store->GetStoreState() == EStoreState::ActiveDynamic ||
        store->GetStoreState() == EStoreState::PassiveDynamic)
    {
        return Eden_.get();
    }

    return GetContainingPartition(store->GetMinKey(), store->GetUpperBoundKey());
}

const TSortedDynamicRowKeyComparer& TTablet::GetRowKeyComparer() const
{
    return RowKeyComparer_;
}

void TTablet::ValidateMountRevision(NHydra::TRevision mountRevision)
{
    if (MountRevision_ != mountRevision) {
        THROW_ERROR_EXCEPTION(
            NTabletClient::EErrorCode::InvalidMountRevision,
            "Invalid mount revision of tablet %v: expected %x, received %x",
            Id_,
            MountRevision_,
            mountRevision)
            << TErrorAttribute("tablet_id", Id_);
    }
}

int TTablet::ComputeEdenOverlappingStoreCount() const
{
    std::map<TLegacyOwningKey, int> keyMap;
    for (const auto& store : Eden_->Stores()) {
        ++keyMap[store->GetMinKey()];
        --keyMap[store->GetUpperBoundKey()];
    }

    int curOverlappingCount = 0;
    int maxOverlappingCount = 0;
    for (const auto& [key, value] : keyMap) {
        Y_UNUSED(key);

        curOverlappingCount += value;
        maxOverlappingCount = std::max(maxOverlappingCount, curOverlappingCount);
    }

    return maxOverlappingCount;
}

int TTablet::ComputeDynamicStoreCount() const
{
    int dynamicStoreCount = 0;

    if (IsPhysicallySorted()) {
        for (const auto& store : Eden_->Stores()) {
            if (store->IsDynamic()) {
                ++dynamicStoreCount;
            }
        }
    } else {
        for (auto it = StoreRowIndexMap_.crbegin(); it != StoreRowIndexMap_.crend(); ++it) {
            if (it->second->IsDynamic()) {
                ++dynamicStoreCount;
            } else {
                break;
            }
        }
    }

    return dynamicStoreCount;
}


void TTablet::UpdateTabletSizeMetrics()
{
    i64 dataWeight = 0;
    i64 uncompressedDataSize = 0;
    i64 compressedDataSize = 0;
    i64 rowCount = 0;
    i64 chunkCount = 0;

    i64 hunkCount = 0;
    i64 totalHunkLength = 0;

    auto handleStore = [&] (const IStorePtr& store) {
        if (store->IsChunk()) {
            dataWeight += store->GetDataWeight();
            uncompressedDataSize += store->GetUncompressedDataSize();
            compressedDataSize += store->GetCompressedDataSize();
            rowCount += store->GetRowCount();
            ++chunkCount;
        }
    };

    if (IsPhysicallySorted()) {
        for (const auto& partition : PartitionList_) {
            for (const auto& store : partition->Stores()) {
                handleStore(store);
            }
        }
        for (const auto& store : Eden_->Stores()) {
            handleStore(store);
        }
    } else {
        for (const auto& [storeId, store] : StoreIdMap_) {
            handleStore(store);
        }
    }

    for (const auto& [hunkChunkId, hunkChunk] : HunkChunkMap_) {
        hunkCount += hunkChunk->GetHunkCount();
        totalHunkLength += hunkChunk->GetTotalHunkLength();
    }

    TabletCounters_.DataWeight.Update(dataWeight);
    TabletCounters_.UncompressedDataSize.Update(uncompressedDataSize);
    TabletCounters_.CompressedDataSize.Update(compressedDataSize);
    TabletCounters_.RowCount.Update(rowCount);
    TabletCounters_.ChunkCount.Update(chunkCount);

    TabletCounters_.HunkCount.Update(hunkCount);
    TabletCounters_.TotalHunkLength.Update(totalHunkLength);
    TabletCounters_.HunkChunkCount.Update(ssize(HunkChunkMap_));
}

void TTablet::UpdateOverlappingStoreCount()
{
    int overlappingStoreCount = 0;
    int criticalPartitionCount = 0;
    for (const auto& partition : PartitionList_) {
        int storeCount = static_cast<int>(partition->Stores().size());
        if (storeCount > overlappingStoreCount) {
            overlappingStoreCount = storeCount;
            criticalPartitionCount = 1;
        } else if (storeCount == overlappingStoreCount) {
            criticalPartitionCount++;
        }
    }
    auto edenOverlappingStoreCount = ComputeEdenOverlappingStoreCount();
    overlappingStoreCount += edenOverlappingStoreCount;

    if (OverlappingStoreCount_ != overlappingStoreCount) {
        YT_LOG_DEBUG("Overlapping store count updated (OverlappingStoreCount: %v)",
            overlappingStoreCount);
    }

    OverlappingStoreCount_ = overlappingStoreCount;
    EdenOverlappingStoreCount_ = edenOverlappingStoreCount;
    CriticalPartitionCount_ = criticalPartitionCount;

    TabletCounters_.OverlappingStoreCount.Update(OverlappingStoreCount_);
    TabletCounters_.EdenStoreCount.Update(GetEdenStoreCount());
}

void TTablet::UpdateUnflushedTimestamp() const
{
    auto unflushedTimestamp = MaxTimestamp;

    for (const auto& [storeId, store] : StoreIdMap()) {
        if (store->IsDynamic()) {
            auto timestamp = store->GetMinTimestamp();
            unflushedTimestamp = std::min(unflushedTimestamp, timestamp);
        }
    }

    if (Context_) {
        auto transactionManager = Context_->GetTransactionManager();
        if (transactionManager) {
            auto prepareTimestamp = transactionManager->GetMinPrepareTimestamp();
            auto commitTimestamp = transactionManager->GetMinCommitTimestamp();
            unflushedTimestamp = std::min({
                unflushedTimestamp,
                prepareTimestamp,
                commitTimestamp});
        }
    }

    RuntimeData_->UnflushedTimestamp = unflushedTimestamp;
}

bool TTablet::IsActiveServant() const
{
    if (SmoothMovementData().GetRole() == ESmoothMovementRole::None) {
        return true;
    }

    return static_cast<bool>(GetMasterAvenueEndpointId());
}

void TTablet::PopulateReplicateTabletContentRequest(NProto::TReqReplicateTabletContent* request)
{
    using NYT::ToProto;

    ToProto(request->mutable_id_generator(), IdGenerator_);

    if (IsPhysicallySorted()) {
        ToProto(request->mutable_eden_id(), Eden_->GetId());

        std::vector<TLegacyKey> pivotKeys;
        for (const auto& partition : PartitionList_) {
            ToProto(request->add_partition_ids(), partition->GetId());
            pivotKeys.push_back(partition->GetPivotKey());
        }

        auto writer = CreateWireProtocolWriter();
        writer->WriteUnversionedRowset(pivotKeys);
        request->set_partition_pivot_keys(MergeRefsToString(writer->Finish()));
    }

    auto* replicatableContent = request->mutable_replicatable_content();
    replicatableContent->set_trimmed_row_count(GetTrimmedRowCount());
    replicatableContent->set_retained_timestamp(RetainedTimestamp_);
    replicatableContent->set_cumulative_data_weight(CumulativeDataWeight_);

    request->set_last_commit_timestamp(GetLastCommitTimestamp());
    request->set_last_write_timestamp(GetLastWriteTimestamp());
}

void TTablet::LoadReplicatedContent(const NProto::TReqReplicateTabletContent* request)
{
    using NYT::FromProto;

    FromProto(&IdGenerator_, request->id_generator());

    if (IsPhysicallySorted()) {
        Eden_ = std::make_unique<TPartition>(
            this,
            FromProto<TPartitionId>(request->eden_id()),
            EdenIndex,
            PivotKey_,
            NextPivotKey_);

        auto partitionIds = FromProto<std::vector<TPartitionId>>(
            request->partition_ids());

        auto reader = CreateWireProtocolReader(
            TSharedRef::FromString(request->partition_pivot_keys()));
        auto partitionPivotKeys = reader->ReadUnversionedRowset(true);

        YT_VERIFY(std::ssize(partitionIds) == std::ssize(partitionPivotKeys));

        PartitionList_.clear();
        PartitionMap_.clear();

        for (int index = 0; index < ssize(partitionIds); ++index) {
            auto partition = std::make_unique<TPartition>(
                this,
                partitionIds[index],
                index,
                TLegacyOwningKey(partitionPivotKeys[index]),
                TLegacyOwningKey(index + 1 < ssize(partitionIds)
                    ? partitionPivotKeys[index + 1]
                    : NextPivotKey_));
            EmplaceOrCrash(PartitionMap_, partition->GetId(), partition.get());
            PartitionList_.push_back(std::move(partition));
        }
    }

    const auto& replicatableContent = request->replicatable_content();
    SetTrimmedRowCount(replicatableContent.trimmed_row_count());
    RetainedTimestamp_ = replicatableContent.retained_timestamp();
    CumulativeDataWeight_ = replicatableContent.cumulative_data_weight();

    RuntimeData_->LastCommitTimestamp = request->last_commit_timestamp();
    RuntimeData_->LastWriteTimestamp = request->last_write_timestamp();
}

i64 TTablet::Lock(ETabletLockType lockType)
{
    ++TabletLockCount_[lockType];

    return ++TotalTabletLockCount_;
}

i64 TTablet::Unlock(ETabletLockType lockType)
{
    YT_ASSERT(TabletLockCount_[lockType] > 0);
    --TabletLockCount_[lockType];

    YT_ASSERT(TotalTabletLockCount_ > 0);
    return --TotalTabletLockCount_;
}

i64 TTablet::GetTotalTabletLockCount() const
{
    return TotalTabletLockCount_;
}

i64 TTablet::GetTransientTabletLockCount() const
{
    auto isTransientLockType = [&] (ETabletLockType lockType) {
        switch (lockType) {
            case ETabletLockType::TransientWrite:
            case ETabletLockType::TransientTransaction:
                return true;
            case ETabletLockType::PersistentTransaction:
                return false;
            default:
                YT_ABORT();
        }
    };

    i64 lockCount = 0;
    for (auto lockType : TEnumTraits<ETabletLockType>::GetDomainValues()) {
        if (isTransientLockType(lockType)) {
            lockCount += TabletLockCount_[lockType];
        }
    }

    return lockCount;
}

i64 TTablet::GetTabletLockCount(ETabletLockType lockType) const
{
    return TabletLockCount_[lockType];
}

int TTablet::GetEdenStoreCount() const
{
    return Eden_->Stores().size();
}

void TTablet::PushDynamicStoreIdToPool(TDynamicStoreId storeId)
{
    YT_VERIFY(storeId);
    DynamicStoreIdPool_.push_back(storeId);
}

TDynamicStoreId TTablet::PopDynamicStoreIdFromPool()
{
    YT_VERIFY(!DynamicStoreIdPool_.empty());
    auto id = DynamicStoreIdPool_.front();
    DynamicStoreIdPool_.pop_front();
    return id;
}

void TTablet::ClearDynamicStoreIdPool()
{
    DynamicStoreIdPool_.clear();
}

TMountHint TTablet::GetMountHint() const
{
    TMountHint mountHint;

    if (IsPhysicallySorted()) {
        std::vector<TStoreId> edenStoreIds;
        for (const auto& store : GetEden()->Stores()) {
            if (store->IsChunk()) {
                edenStoreIds.push_back(store->GetId());
            }
        }
        std::sort(edenStoreIds.begin(), edenStoreIds.end());
        ToProto(mountHint.mutable_eden_store_ids(), edenStoreIds);
    }

    return mountHint;
}

TConsistentReplicaPlacementHash TTablet::GetConsistentChunkReplicaPlacementHash() const
{
    if (!Settings_.MountConfig->EnableConsistentChunkReplicaPlacement) {
        return NullConsistentReplicaPlacementHash;
    }

    auto hash = Id_.Parts64[0] ^ Id_.Parts64[1];
    if (hash == NullConsistentReplicaPlacementHash) {
        // Unbelievable.
        hash = 1;
    }
    return hash;
}

void TTablet::SetBackupCheckpointTimestamp(TTimestamp timestamp)
{
    BackupMetadata().SetCheckpointTimestamp(timestamp);
    if (const auto& activeStore = GetActiveStore()) {
        activeStore->SetBackupCheckpointTimestamp(timestamp);
    }
    RuntimeData()->BackupCheckpointTimestamp.store(timestamp);
}

TTimestamp TTablet::GetBackupCheckpointTimestamp() const
{
    return BackupMetadata().GetCheckpointTimestamp();
}

EBackupMode TTablet::GetBackupMode() const
{
    return BackupMetadata().GetBackupMode();
}

void TTablet::SetBackupStage(EBackupStage stage)
{
    BackupMetadata().SetBackupStage(stage);
}

EBackupStage TTablet::GetBackupStage() const
{
    return BackupMetadata().GetBackupStage();
}

void TTablet::ThrottleTabletStoresUpdate(
    const ITabletSlotPtr& slot,
    const NLogging::TLogger& Logger) const
{
    const auto& throttler = DistributedThrottlers()[ETabletDistributedThrottlerKind::StoresUpdate];
    if (!throttler) {
        return;
    }

    NProfiling::TWallTimer timer;

    YT_LOG_DEBUG("Started waiting for tablet stores update throttler (CellId: %v)",
        slot->GetCellId());

    auto asyncResult = throttler->Throttle(1);
    auto result = asyncResult.IsSet()
        ? asyncResult.Get()
        : WaitFor(asyncResult);
    result.ThrowOnError();

    auto elapsedTime = timer.GetElapsedTime();
    YT_LOG_DEBUG("Finished waiting for tablet stores update throttler (ElapsedTime: %v, CellId: %v)",
        elapsedTime,
        slot->GetCellId());
    TableProfiler_->GetThrottlerTimer(ETabletDistributedThrottlerKind::StoresUpdate)
        ->Record(elapsedTime);
}

void TTablet::UpdateDanglingHunkChunks(const THunkChunkPtr& hunkChunk)
{
    if (hunkChunk->IsDangling()) {
        // NB: May already be there.
        DanglingHunkChunks_.insert(hunkChunk);
    } else {
        // NB: May be missing.
        DanglingHunkChunks_.erase(hunkChunk);
    }
}

TTabletHunkWriterOptionsPtr TTablet::CreateFallbackHunkWriterOptions(const TTabletStoreWriterOptionsPtr& storeWriterOptions)
{
    auto hunkWriterOptions = New<TTabletHunkWriterOptions>();
    hunkWriterOptions->ReplicationFactor = storeWriterOptions->ReplicationFactor;
    hunkWriterOptions->MediumName = storeWriterOptions->MediumName;
    hunkWriterOptions->Account = storeWriterOptions->Account;
    hunkWriterOptions->CompressionCodec = storeWriterOptions->CompressionCodec;
    hunkWriterOptions->ErasureCodec = storeWriterOptions->ErasureCodec;
    hunkWriterOptions->ChunksVital = storeWriterOptions->ChunksVital;
    return hunkWriterOptions;
}

const TRowCachePtr& TTablet::GetRowCache() const
{
    return RowCache_;
}

void TTablet::RecomputeReplicaStatuses()
{
    for (auto& [replicaId, replicaInfo] : Replicas()) {
        replicaInfo.RecomputeReplicaStatus();
    }
}

void TTablet::RecomputeCommittedReplicationRowIndices()
{
    for (auto& [replica, replicaInfo] : Replicas()) {
        auto committedReplicationRowIndex = replicaInfo.GetCurrentReplicationRowIndex();
        if (replicaInfo.GetMode() == ETableReplicaMode::Sync) {
            committedReplicationRowIndex -= GetDelayedLocklessRowCount();
        }
        replicaInfo.SetCommittedReplicationRowIndex(committedReplicationRowIndex);
    }
}

void TTablet::CheckedSetBackupStage(EBackupStage previous, EBackupStage next)
{
    YT_LOG_DEBUG("Tablet backup stage changed (%v, BackupStage: %v -> %v)",
        GetLoggingTag(),
        previous,
        next);
    YT_VERIFY(GetBackupStage() == previous);
    SetBackupStage(next);
}

void TTablet::RecomputeNonActiveStoresUnmergedRowCount()
{
    i64 nonActiveStoresUnmergedRowCount = 0;
    for (const auto& store : StoreIdMap_) {
        if (store.second != ActiveStore_) {
            nonActiveStoresUnmergedRowCount += store.second->GetRowCount();
        }
    }

    NonActiveStoresUnmergedRowCount_ = nonActiveStoresUnmergedRowCount;
}

void TTablet::UpdateUnmergedRowCount()
{
    if (RowCache_) {
        double lookupCacheRowsRatio = Settings_.MountConfig->LookupCacheRowsRatio;

        if (lookupCacheRowsRatio > 0) {
            i64 unmergedRowCount = NonActiveStoresUnmergedRowCount_;
            if (ActiveStore_) {
                unmergedRowCount += ActiveStore_->GetRowCount();
            }

            i64 lookupCacheCapacity = lookupCacheRowsRatio * unmergedRowCount;
            RowCache_->GetCache()->SetCapacity(std::max<i64>(lookupCacheCapacity, 1));
        }
    }
}

TTimestamp TTablet::GetOrderedChaosReplicationMinTimestamp()
{
    YT_VERIFY(!TableSchema_->IsSorted());

    auto replicationProgress = RuntimeData()->ReplicationProgress.Acquire();
    auto replicationCard = RuntimeData()->ReplicationCard.Acquire();

    if (!replicationProgress ||
        !IsOrderedTabletReplicationProgress(*replicationProgress) ||
        !replicationCard ||
        replicationCard->Replicas.empty())
    {
        return MinTimestamp;
    }

    const auto& segments = replicationProgress->Segments;
    const auto& upper = replicationProgress->UpperKey;
    auto replicationTimestamp = MaxTimestamp;
    for (const auto& [_, replica] : replicationCard->Replicas) {
        auto minTimestamp = GetReplicationProgressMinTimestamp(
            replica.ReplicationProgress,
            segments[0].LowerKey,
            upper);
        replicationTimestamp = std::min(replicationTimestamp, minTimestamp);
    }

    return replicationTimestamp;
}

const IHunkLockManagerPtr& TTablet::GetHunkLockManager() const
{
    return HunkLockManager_;
}

bool TTablet::IsDictionaryBuildingInProgress(
    NTableClient::EDictionaryCompressionPolicy policy) const
{
    return CompressionDictionaryInfos_[policy].BuildingInProgress;
}

void TTablet::SetDictionaryBuildingInProgress(
    NTableClient::EDictionaryCompressionPolicy policy, bool flag)
{
    CompressionDictionaryInfos_[policy].BuildingInProgress = flag;
}

TInstant TTablet::GetCompressionDictionaryRebuildBackoffTime(
    NTableClient::EDictionaryCompressionPolicy policy) const
{
    return CompressionDictionaryInfos_[policy].RebuildBackoffTime;
}

void TTablet::SetCompressionDictionaryRebuildBackoffTime(
    EDictionaryCompressionPolicy policy,
    TInstant backoffTime)
{
    CompressionDictionaryInfos_[policy].RebuildBackoffTime = backoffTime;
}

////////////////////////////////////////////////////////////////////////////////

void BuildTableSettingsOrchidYson(const TTableSettings& options, NYTree::TFluentMap fluent)
{
    fluent
        .Item("config")
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .Value(options.MountConfig)
        .Item("store_writer_config")
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .Value(options.StoreWriterConfig)
        .Item("store_writer_options")
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .Value(options.StoreWriterOptions)
        .Item("hunk_writer_config")
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .Value(options.HunkWriterConfig)
        .Item("hunk_writer_options")
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .Value(options.HunkWriterOptions)
        .Item("store_reader_config")
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .Value(options.StoreReaderConfig)
        .Item("hunk_reader_config")
            .BeginAttributes()
                .Item("opaque").Value(true)
            .EndAttributes()
            .Value(options.HunkReaderConfig);
}

IThroughputThrottlerPtr GetBlobMediumWriteThrottler(
    const NClusterNode::TClusterNodeDynamicConfigManagerPtr& dynamicConfigManager,
    const TTabletSnapshotPtr& tabletSnapshot)
{
    auto mediumThrottlersConfig = dynamicConfigManager->GetConfig()->TabletNode->MediumThrottlers;
    if (!mediumThrottlersConfig->EnableBlobThrottling) {
        return GetUnlimitedThrottler();
    }

    return tabletSnapshot->DistributedThrottlers[ETabletDistributedThrottlerKind::BlobMediumWrite];
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
