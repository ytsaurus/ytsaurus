#include "tablet.h"

#include "automaton.h"
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

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/ytlib/query_client/column_evaluator.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/tls_cache.h>

namespace NYT::NTabletNode {

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

using NProto::TMountHint;

////////////////////////////////////////////////////////////////////////////////

void ValidateTabletRetainedTimestamp(const TTabletSnapshotPtr& tabletSnapshot, TTimestamp timestamp)
{
    if (timestamp < tabletSnapshot->RetainedTimestamp) {
        THROW_ERROR_EXCEPTION("Timestamp %llx is less than tablet %v retained timestamp %llx",
            timestamp,
            tabletSnapshot->TabletId,
            tabletSnapshot->RetainedTimestamp);
    }
}

////////////////////////////////////////////////////////////////////////////////

TDeleteListFlusher::~TDeleteListFlusher()
{
    FlushDeleteList();
}

TRowCache::TRowCache(
    size_t elementCount,
    const NProfiling::TProfiler& profiler,
    IMemoryUsageTrackerPtr memoryTracker)
    : Allocator(profiler, std::move(memoryTracker))
    , Cache(elementCount)
{ }

////////////////////////////////////////////////////////////////////////////////

void TRuntimeTableReplicaData::Populate(TTableReplicaStatistics* statistics) const
{
    statistics->set_current_replication_row_index(CurrentReplicationRowIndex.load());
    statistics->set_current_replication_timestamp(CurrentReplicationTimestamp.load());
}

void TRuntimeTableReplicaData::MergeFrom(const TTableReplicaStatistics& statistics)
{
    CurrentReplicationRowIndex = statistics.current_replication_row_index();
    CurrentReplicationTimestamp = statistics.current_replication_timestamp();
}

////////////////////////////////////////////////////////////////////////////////

TTableSettings TTableSettings::CreateNew()
{
    return {
        .MountConfig = New<TTableMountConfig>(),
        .StoreReaderConfig = New<TTabletStoreReaderConfig>(),
        .HunkReaderConfig = New<TTabletHunkReaderConfig>(),
        .StoreWriterConfig = New<TTabletStoreWriterConfig>(),
        .StoreWriterOptions = New<TTabletStoreWriterOptions>(),
        .HunkWriterConfig = New<TTabletHunkWriterConfig>(),
        .HunkWriterOptions = New<TTabletHunkWriterOptions>()
    };
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

    return std::make_pair(beginIt, endIt);
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
            "Invalid mount revision of tablet %v: expected %llx, received %llx",
            TabletId,
            MountRevision,
            mountRevision)
            << TErrorAttribute("tablet_id", TabletId);
    }
}

void TTabletSnapshot::WaitOnLocks(TTimestamp timestamp) const
{
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

////////////////////////////////////////////////////////////////////////////////

TTablet::TTablet(
    TTabletId tabletId,
    ITabletContext* context)
    : TObjectBase(tabletId)
    , Context_(context)
    , LockManager_(New<TLockManager>())
    , Logger(TabletNodeLogger.WithTag("TabletId: %v", Id_))
    , Settings_(TTableSettings::CreateNew())
{
    Settings_.MountConfig = New<TTableMountConfig>();
    Settings_.StoreReaderConfig = New<TTabletStoreReaderConfig>();
    Settings_.HunkReaderConfig = New<TTabletHunkReaderConfig>();
    Settings_.StoreWriterConfig = New<TTabletStoreWriterConfig>();
    Settings_.StoreWriterOptions = New<TTabletStoreWriterOptions>();
    Settings_.HunkWriterConfig = New<TTabletHunkWriterConfig>();
    Settings_.HunkWriterOptions = New<TTabletHunkWriterOptions>();
}

TTablet::TTablet(
    TTabletId tabletId,
    TTableSettings settings,
    NHydra::TRevision mountRevision,
    TObjectId tableId,
    const TYPath& path,
    ITabletContext* context,
    TObjectId schemaId,
    TTableSchemaPtr schema,
    TLegacyOwningKey pivotKey,
    TLegacyOwningKey nextPivotKey,
    EAtomicity atomicity,
    ECommitOrdering commitOrdering,
    TTableReplicaId upstreamReplicaId,
    TTimestamp retainedTimestamp)
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
    , Context_(context)
    , LockManager_(New<TLockManager>())
    , Logger(TabletNodeLogger.WithTag("TabletId: %v", Id_))
    , Settings_(std::move(settings))
    , Eden_(std::make_unique<TPartition>(
        this,
        context->GenerateId(EObjectType::TabletPartition),
        EdenIndex,
        PivotKey_,
        NextPivotKey_))
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

const TLockManagerPtr& TTablet::GetLockManager() const
{
    return LockManager_;
}

void TTablet::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, TableId_);
    Save(context, MountRevision_);
    Save(context, TablePath_);
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

    TSizeSerializer::Save(context, StoreIdMap_.size());
    // NB: This is not stable.
    for (const auto& [storeId, store] : StoreIdMap_) {
        Save(context, store->GetType());
        Save(context, storeId);
        store->Save(context);
    }

    TSizeSerializer::Save(context, HunkChunkMap_.size());
    // NB: This is not stable.
    for (const auto& [chunkId, hunkChunk] : HunkChunkMap_) {
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
}

void TTablet::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, TableId_);
    Load(context, MountRevision_);
    Load(context, TablePath_);
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
    for (auto& [_, replicaInfo] : Replicas_) {
        replicaInfo.SetTablet(this);
    }
    Load(context, RetainedTimestamp_);

    // NB: Stores that we're about to create may request some tablet properties (e.g. column lock count)
    // during construction. Initialize() will take care of this.
    Initialize();

    int storeCount = TSizeSerializer::LoadSuspended(context);
    SERIALIZATION_DUMP_WRITE(context, "stores[%v]", storeCount);
    SERIALIZATION_DUMP_INDENT(context) {
        for (int index = 0; index < storeCount; ++index) {
            auto storeType = Load<EStoreType>(context);
            auto storeId = Load<TStoreId> (context);
            auto store = Context_->CreateStore(this, storeType, storeId, nullptr);
            YT_VERIFY(StoreIdMap_.emplace(storeId, store).second);
            store->Load(context);
            if (store->IsChunk()) {
                YT_VERIFY(store->AsChunk()->GetChunkId());
            }
        }
    }

    // COMPAT(babenko)
    if (context.GetVersion() >= ETabletReign::Hunks1) {
        int hunkChunkCount = TSizeSerializer::LoadSuspended(context);
        SERIALIZATION_DUMP_WRITE(context, "hunk_chunks[%v]", hunkChunkCount);
        SERIALIZATION_DUMP_INDENT(context) {
            for (int index = 0; index < hunkChunkCount; ++index) {
                auto chunkId = Load<TChunkId>(context);
                auto hunkChunk = Context_->CreateHunkChunk(this, chunkId, nullptr);
                YT_VERIFY(HunkChunkMap_.emplace(chunkId, hunkChunk).second);
                hunkChunk->Load(context);
                hunkChunk->Initialize();
                UpdateDanglingHunkChunks(hunkChunk);
            }
        }
    }

    if (IsPhysicallyOrdered()) {
        for (const auto& [storeId, store] : StoreIdMap_) {
            auto orderedStore = store->AsOrdered();
            YT_VERIFY(StoreRowIndexMap_.emplace(orderedStore->GetStartingRowIndex(), orderedStore).second);
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
            YT_VERIFY(PartitionMap_.emplace(partition->GetId(), partition.get()).second);
            PartitionList_.push_back(std::move(partition));
        }
    }

    // COMPAT(savrus)
    if (context.GetVersion() >= ETabletReign::BulkInsert) {
        Load(context, *LockManager_);
    }

    // COMPAT(ifsmirnov)
    if (context.GetVersion() >= ETabletReign::DynamicStoreRead) {
        Load(context, DynamicStoreIdPool_);
        Load(context, DynamicStoreIdRequested_);
    }

    // COMPAT(akozhikhov)
    if (context.GetVersion() >= ETabletReign::SchemaIdUponMount) {
        Load(context, SchemaId_);
    }

    UpdateOverlappingStoreCount();
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

    return BIND(
        [
            snapshot = BuildSnapshot(nullptr),
            capturedStores = std::move(capturedStores),
            capturedEden = std::move(capturedEden),
            capturedPartitions = std::move(capturedPartitions)
        ] (TSaveContext& context) {
            using NYT::Save;

            Save(context, *snapshot->Settings.MountConfig);
            Save(context, *snapshot->Settings.StoreReaderConfig);
            Save(context, *snapshot->Settings.HunkReaderConfig);
            Save(context, *snapshot->Settings.StoreWriterConfig);
            Save(context, *snapshot->Settings.StoreWriterOptions);
            Save(context, *snapshot->Settings.HunkWriterConfig);
            Save(context, *snapshot->Settings.HunkWriterOptions);
            Save(context, snapshot->PivotKey);
            Save(context, snapshot->NextPivotKey);

            capturedEden.Run(context);

            for (const auto& callback : capturedPartitions) {
                callback.Run(context);
            }

            // NB: This is not stable.
            for (const auto& [storeId, callback] : capturedStores) {
                Save(context, storeId);
                callback(context);
            }
        });
}

void TTablet::AsyncLoad(TLoadContext& context)
{
    using NYT::Load;

    Load(context, *Settings_.MountConfig);
    // COMPAT(babenko)
    if (context.GetVersion() >= ETabletReign::Hunks2) {
        Load(context, *Settings_.StoreReaderConfig);
        Load(context, *Settings_.HunkReaderConfig);
    }
    Load(context, *Settings_.StoreWriterConfig);
    Load(context, *Settings_.StoreWriterOptions);
    // COMPAT(babenko)
    if (context.GetVersion() >= ETabletReign::Hunks1) {
        Load(context, *Settings_.HunkWriterConfig);
        Load(context, *Settings_.HunkWriterOptions);
    } else {
        Settings_.HunkWriterOptions = CreateFallbackHunkWriterOptions(Settings_.StoreWriterOptions);
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
            auto storeId = Load<TStoreId>(context);
            SERIALIZATION_DUMP_WRITE(context, "%v =>", storeId);
            SERIALIZATION_DUMP_INDENT(context) {
                auto store = GetStore(storeId);
                store->AsyncLoad(context);
                store->Initialize();
            }
        }
    }

    ReconfigureThrottlers();
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
        Context_->GenerateId(EObjectType::TabletPartition),
        static_cast<int>(PartitionList_.size()),
        PivotKey_,
        NextPivotKey_);
    YT_VERIFY(PartitionMap_.emplace(partition->GetId(), partition.get()).second);
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

void TTablet::MergePartitions(int firstIndex, int lastIndex)
{
    YT_VERIFY(IsPhysicallySorted());

    for (int i = lastIndex + 1; i < static_cast<int>(PartitionList_.size()); ++i) {
        PartitionList_[i]->SetIndex(i - (lastIndex - firstIndex));
    }

    auto mergedPartition = std::make_unique<TPartition>(
        this,
        Context_->GenerateId(EObjectType::TabletPartition),
        firstIndex,
        PartitionList_[firstIndex]->GetPivotKey(),
        PartitionList_[lastIndex]->GetNextPivotKey());

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
            YT_VERIFY(mergedPartition->Stores().insert(store).second);
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
    YT_VERIFY(PartitionMap_.emplace(mergedPartition->GetId(), mergedPartition.get()).second);
    PartitionList_.erase(firstPartitionIt, lastPartitionIt + 1);
    PartitionList_.insert(firstPartitionIt, std::move(mergedPartition));

    StructuredLogger_->OnPartitionsMerged(
        existingPartitionIds,
        PartitionList_[firstIndex].get());

    UpdateOverlappingStoreCount();
}

void TTablet::SplitPartition(int index, const std::vector<TLegacyOwningKey>& pivotKeys)
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
            Context_->GenerateId(EObjectType::TabletPartition),
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

        splitPartitions.push_back(std::move(partition));
    }

    PartitionMap_.erase(existingPartition->GetId());
    for (const auto& partition : splitPartitions) {
        YT_VERIFY(PartitionMap_.emplace(partition->GetId(), partition.get()).second);
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
        YT_VERIFY(newPartition->Stores().insert(store).second);
    }

    StructuredLogger_->OnPartitionSplit(
        existingPartition.get(),
        index,
        pivotKeys.size());

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

void TTablet::AddStore(IStorePtr store)
{
    YT_VERIFY(StoreIdMap_.emplace(store->GetId(), store).second);
    if (IsPhysicallySorted()) {
        auto sortedStore = store->AsSorted();
        auto* partition = GetContainingPartition(sortedStore);
        YT_VERIFY(partition->Stores().insert(sortedStore).second);
        sortedStore->SetPartition(partition);
        UpdateOverlappingStoreCount();
    } else {
        auto orderedStore = store->AsOrdered();
        YT_VERIFY(StoreRowIndexMap_.emplace(orderedStore->GetStartingRowIndex(), orderedStore).second);
    }
}

void TTablet::RemoveStore(IStorePtr store)
{
    YT_VERIFY(StoreIdMap_.erase(store->GetId()) == 1);
    if (IsPhysicallySorted()) {
        auto sortedStore = store->AsSorted();
        auto* partition = sortedStore->GetPartition();
        YT_VERIFY(partition->Stores().erase(sortedStore) == 1);
        sortedStore->SetPartition(nullptr);
        UpdateOverlappingStoreCount();
    } else {
        auto orderedStore = store->AsOrdered();
        YT_VERIFY(StoreRowIndexMap_.erase(orderedStore->GetStartingRowIndex()) == 1);
    }
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
    YT_VERIFY(HunkChunkMap_.emplace(hunkChunk->GetId(), hunkChunk).second);
}

void TTablet::RemoveHunkChunk(THunkChunkPtr hunkChunk)
{
    YT_VERIFY(HunkChunkMap_.erase(hunkChunk->GetId()) == 1);
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

int TTablet::GetColumnLockCount() const
{
    return LockIndexToName_.size();
}

i64 TTablet::GetTotalRowCount() const
{
    return RuntimeData_->TotalRowCount;
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
    return DelayedLocklessRowCount_;
}

void TTablet::SetDelayedLocklessRowCount(i64 value)
{
    DelayedLocklessRowCount_ = value;
}

i64 TTablet::GetTrimmedRowCount() const
{
    return RuntimeData_->TrimmedRowCount;
}

void TTablet::SetTrimmedRowCount(i64 value)
{
    RuntimeData_->TrimmedRowCount = value;
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

void TTablet::StartEpoch(const ITabletSlotPtr& slot)
{
    CancelableContext_ = New<TCancelableContext>();

    for (auto queue : TEnumTraits<EAutomatonThreadQueue>::GetDomainValues()) {
        EpochAutomatonInvokers_[queue] = CancelableContext_->CreateInvoker(
            // NB: Slot can be null in tests.
            slot
            ? slot->GetEpochAutomatonInvoker(queue)
            : GetSyncInvoker());
    }

    Eden_->StartEpoch();
    for (const auto& partition : PartitionList_) {
        partition->StartEpoch();
    }

    if (slot) {
        ChunkFragmentReader_ = slot->CreateChunkFragmentReader(this);
    }
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

    ChunkFragmentReader_.Reset();
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
    }

    snapshot->TabletId = Id_;
    snapshot->LoggingTag = LoggingTag_;
    snapshot->MountRevision = MountRevision_;
    snapshot->TablePath = TablePath_;
    snapshot->TableId = TableId_;
    snapshot->Settings = Settings_;
    snapshot->PivotKey = PivotKey_;
    snapshot->NextPivotKey = NextPivotKey_;
    snapshot->TableSchema = TableSchema_;
    snapshot->PhysicalSchema = PhysicalSchema_;
    snapshot->QuerySchema = PhysicalSchema_->ToQuery();
    snapshot->PhysicalSchemaData = PhysicalSchemaData_;
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

    for (const auto& [replicaId, replicaInfo] : Replicas_) {
        YT_VERIFY(snapshot->Replicas.emplace(replicaId, replicaInfo.BuildSnapshot()).second);
    }

    UpdateUnflushedTimestamp();

    snapshot->TableProfiler = TableProfiler_;

    snapshot->ConsistentChunkReplicaPlacementHash = GetConsistentChunkReplicaPlacementHash();

    snapshot->ChunkFragmentReader = GetChunkFragmentReader();

    return snapshot;
}

void TTablet::Initialize()
{
    PhysicalSchema_ = IsReplicated() ? TableSchema_->ToReplicationLog() : TableSchema_;

    PhysicalSchemaData_ = TWireProtocolReader::GetSchemaData(*PhysicalSchema_);
    KeysSchemaData_ = TWireProtocolReader::GetSchemaData(*PhysicalSchema_->ToKeys());

    RowKeyComparer_ = Context_->GetRowComparerProvider()->Get(GetKeyColumnTypes(PhysicalSchema_));

    auto groupToIndex = GetLocksMapping(
        *PhysicalSchema_,
        Atomicity_ == EAtomicity::Full,
        &ColumnIndexToLockIndex_,
        &LockIndexToName_);

    ColumnEvaluator_ = Context_->GetColumnEvaluatorCache()->Find(PhysicalSchema_);

    StoresUpdateCommitSemaphore_ = New<NConcurrency::TAsyncSemaphore>(1);

    FlushThrottler_ = CreateReconfigurableThroughputThrottler(
        Settings_.MountConfig->FlushThrottler,
        Logger);
    CompactionThrottler_ = CreateReconfigurableThroughputThrottler(
        Settings_.MountConfig->CompactionThrottler,
        Logger);
    PartitioningThrottler_ = CreateReconfigurableThroughputThrottler(
        Settings_.MountConfig->PartitioningThrottler,
        Logger);

    LoggingTag_ = Format("TabletId: %v, TableId: %v, TablePath: %v",
        Id_,
        TableId_,
        TablePath_);
}

void TTablet::ConfigureRowCache()
{
    if (Settings_.MountConfig->LookupCacheRowsPerTablet > 0) {
        if (!RowCache_) {
            RowCache_ = New<TRowCache>(
                Settings_.MountConfig->LookupCacheRowsPerTablet,
                TabletNodeProfiler.WithTag("table_path", TablePath_),
                Context_
                    ->GetMemoryUsageTracker()
                    ->WithCategory(NNodeTrackerClient::EMemoryCategory::LookupRowsCache));
        } else {
            RowCache_->Cache.SetCapacity(Settings_.MountConfig->LookupCacheRowsPerTablet);
        }
    } else if (RowCache_) {
        RowCache_.Reset();
    }
}

void TTablet::ResetRowCache()
{
    RowCache_.Reset();
}

void TTablet::FillProfilerTags()
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
}

void TTablet::ReconfigureThrottlers()
{
    FlushThrottler_->Reconfigure(Settings_.MountConfig->FlushThrottler);
    CompactionThrottler_->Reconfigure(Settings_.MountConfig->CompactionThrottler);
    PartitioningThrottler_->Reconfigure(Settings_.MountConfig->PartitioningThrottler);
}

void TTablet::ReconfigureDistributedThrottlers(const IDistributedThrottlerManagerPtr& throttlerManager)
{
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
            /* admitUnlimitedThrottler */ true);

    DistributedThrottlers_[ETabletDistributedThrottlerKind::Lookup] =
        throttlerManager->GetOrCreateThrottler(
            TablePath_,
            CellTagFromId(Id_),
            getThrottlerConfig("lookup"),
            "lookup",
            EDistributedThrottlerMode::Adaptive,
            LookupThrottlerRpcTimeout,
            /* admitUnlimitedThrottler */ false);
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
            /* admitUnlimitedThrottler */ false);

    DistributedThrottlers_[ETabletDistributedThrottlerKind::CompactionRead] =
        throttlerManager->GetOrCreateThrottler(
            TablePath_,
            CellTagFromId(Id_),
            getThrottlerConfig("compaction_read"),
            "compaction_read",
            EDistributedThrottlerMode::Adaptive,
            CompactionReadThrottlerRpcTimeout,
            /* admitUnlimitedThrottler */ false);

    DistributedThrottlers_[ETabletDistributedThrottlerKind::Write] =
        throttlerManager->GetOrCreateThrottler(
            TablePath_,
            CellTagFromId(Id_),
            getThrottlerConfig("write"),
            "write",
            EDistributedThrottlerMode::Adaptive,
            WriteThrottlerRpcTimeout,
            /* admitUnlimitedThrottler */ false);
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
            "Invalid mount revision of tablet %v: expected %llx, received %llx",
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

i64 TTablet::Lock()
{
    return ++TabletLockCount_;
}

i64 TTablet::Unlock()
{
    YT_ASSERT(TabletLockCount_ > 0);

    return --TabletLockCount_;
}

i64 TTablet::GetTabletLockCount() const
{
    return TabletLockCount_;
}

int TTablet::GetEdenStoreCount() const
{
    return Eden_->Stores().size();
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

void TTablet::PushDynamicStoreIdToPool(TDynamicStoreId storeId)
{
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

