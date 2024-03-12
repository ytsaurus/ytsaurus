#include "in_memory_manager.h"
#include "sorted_chunk_store.h"
#include "sorted_dynamic_store.h"
#include "sorted_store_manager.h"
#include "tablet.h"
#include "tablet_profiling.h"
#include "tablet_slot.h"
#include "transaction_manager.h"
#include "structured_logger.h"
#include "automaton.h"

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>
#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/confirming_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/chunk_replica_cache.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/yt/ytlib/table_client/versioned_row_merger.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/versioned_writer.h>

#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/finally.h>

#include <util/generic/cast.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NYTree;
using namespace NApi;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NTabletNode::NProto;
using namespace NHydra;

using NTableClient::TLegacyKey;
using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

struct THunkScratchBufferTag
{ };

struct TMergeRowsOnFlushBufferTag
{ };

struct TMergeRowsOnFlushTag
{ };

static const size_t MaxRowsPerFlushRead = 1024;

////////////////////////////////////////////////////////////////////////////////

TSortedStoreManager::TSortedStoreManager(
    TTabletManagerConfigPtr config,
    TTablet* tablet,
    ITabletContext* tabletContext,
    NHydra::IHydraManagerPtr hydraManager,
    IInMemoryManagerPtr inMemoryManager,
    NNative::IClientPtr client)
    : TStoreManagerBase(
        std::move(config),
        tablet,
        tabletContext,
        std::move(hydraManager),
        std::move(inMemoryManager),
        std::move(client))
    , KeyColumnCount_(Tablet_->GetPhysicalSchema()->GetKeyColumnCount())
{
    for (const auto& [storeId, store] : Tablet_->StoreIdMap()) {
        auto sortedStore = store->AsSorted();
        if (sortedStore->GetStoreState() != EStoreState::ActiveDynamic) {
            MaxTimestampToStore_.emplace(sortedStore->GetMaxTimestamp(), sortedStore);
        }

        if (sortedStore->IsDynamic()) {
            auto sortedDynamicStore = sortedStore->AsSortedDynamic();
            YT_VERIFY(sortedDynamicStore->GetFlushIndex() == 0);
        }
    }

    if (Tablet_->GetActiveStore()) {
        ActiveStore_ = Tablet_->GetActiveStore()->AsSortedDynamic();
    }
}

bool TSortedStoreManager::ExecuteWrites(
    IWireProtocolReader* reader,
    TWriteContext* context)
{
    while (!reader->IsFinished()) {
        TSortedDynamicRowRef rowRef;
        auto readerCheckpoint = reader->GetCurrent();

        auto command = reader->ReadWriteCommand(
            Tablet_->TableSchemaData(),
            /*captureValues*/ false);

        Visit(command,
            [&] (const TWriteRowCommand& command) {
                rowRef = ModifyRow(command.Row, ERowModificationType::Write, TLockMask(), context);
            },
            [&] (const TDeleteRowCommand& command) {
                rowRef = ModifyRow(command.Row, ERowModificationType::Delete, TLockMask(), context);
            },
            [&] (const TVersionedWriteRowCommand& command) {
                rowRef = ModifyRow(command.VersionedRow, context);
            },
            [&] (const TWriteAndLockRowCommand& command) {
                rowRef = ModifyRow(command.Row, ERowModificationType::WriteAndLock, command.LockMask, context);
            },
            [&] (const auto& command) {
                THROW_ERROR_EXCEPTION("Unsupported write command %v",
                    GetWireProtocolCommand(command));
            });

        if (!rowRef) {
            reader->SetCurrent(readerCheckpoint);
            return false;
        }
    }

    Tablet_->UpdateUnmergedRowCount();

    return true;
}

TSortedDynamicRowRef TSortedStoreManager::ModifyRow(
    TUnversionedRow row,
    ERowModificationType modificationType,
    TLockMask lockMask,
    TWriteContext* context)
{
    auto phase = context->Phase;
    auto atomic = Tablet_->GetAtomicity() == EAtomicity::Full;

    switch (modificationType) {
        case ERowModificationType::Write:
        case ERowModificationType::WriteAndLock: {
            if (Tablet_->GetAtomicity() == EAtomicity::None) {
                break;
            }
            const auto& columnIndexToLockIndex = Tablet_->ColumnIndexToLockIndex();
            for (int index = KeyColumnCount_; index < static_cast<int>(row.GetCount()); ++index) {
                const auto& value = row[index];
                int lockIndex = columnIndexToLockIndex[value.Id];

                if (lockMask.Get(lockIndex) != ELockType::Exclusive &&
                    lockMask.Get(lockIndex) != ELockType::SharedWrite)
                {
                    lockMask.Set(lockIndex, ELockType::Exclusive);
                }
            }
            break;
        }
        case ERowModificationType::Delete:
            lockMask.Set(PrimaryLockIndex, ELockType::Exclusive);
            break;
        default:
            YT_ABORT();
    }

    if (atomic &&
        phase == EWritePhase::Prelock &&
        !CheckInactiveStoresLocks(row, lockMask, context))
    {
        return TSortedDynamicRowRef();
    }

    if (!atomic) {
        YT_ASSERT(phase == EWritePhase::Commit);
        context->CommitTimestamp = GenerateMonotonicCommitTimestamp(context->CommitTimestamp);
    }

    auto isDelete = modificationType == ERowModificationType::Delete;

    auto dynamicRow = ActiveStore_->ModifyRow(row, lockMask, isDelete, context);
    if (!dynamicRow) {
        return TSortedDynamicRowRef();
    }

    auto dynamicRowRef = TSortedDynamicRowRef(ActiveStore_.Get(), this, dynamicRow);
    dynamicRowRef.LockMask = lockMask;

    if (atomic && (phase == EWritePhase::Prelock || phase == EWritePhase::Lock)) {
        LockRow(context, phase == EWritePhase::Prelock, dynamicRowRef);
    }

    return dynamicRowRef;
}

TSortedDynamicRowRef TSortedStoreManager::ModifyRow(
    TVersionedRow row,
    TWriteContext* context)
{
    auto dynamicRow = ActiveStore_->ModifyRow(row, context);
    return TSortedDynamicRowRef(ActiveStore_.Get(), this, dynamicRow);
}

void TSortedStoreManager::LockRow(TWriteContext* context, bool prelock, const TSortedDynamicRowRef& rowRef)
{
    if (prelock) {
        context->PrelockedRows->push(rowRef);
    } else {
        context->LockedRows->push_back(rowRef);
    }
}

void TSortedStoreManager::ConfirmRow(TWriteContext* context, const TSortedDynamicRowRef& rowRef)
{
    for (int lockIndex = 0; lockIndex < rowRef.LockMask.GetSize(); ++lockIndex) {
        if (rowRef.LockMask.Get(lockIndex) == ELockType::SharedWrite) {
            context->HasSharedWriteLocks = true;
            break;
        }
    }

    context->LockedRows->push_back(rowRef);
}

void TSortedStoreManager::PrepareRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
{
    rowRef.Store->PrepareRow(transaction, rowRef.Row);
}

bool TSortedStoreManager::CommitRow(
    TTransaction* transaction,
    const NTableClient::TWireProtocolWriteCommand& command,
    const TSortedDynamicRowRef& rowRef)
{
    bool keyDiffers = false;
    auto validateWireKey = [&] (TUnversionedRow row) {
        const auto& comparer = ActiveStore_->GetRowKeyComparer();
        if (comparer(rowRef.Row, ToKeyRef(row, KeyColumnCount_)) != 0) {
            keyDiffers = true;
        }
    };

    Visit(command,
        [&] (const TWriteRowCommand& command) { validateWireKey(command.Row); },
        [&] (const TDeleteRowCommand& command) { validateWireKey(command.Row); },
        [&] (const TWriteAndLockRowCommand& command) { validateWireKey(command.Row); },
        [&] (auto) { YT_ABORT(); });

    if (keyDiffers) {
        return false;
    }

    auto applyCommand = [&] (
        const TSortedDynamicStorePtr& store,
        TSortedDynamicRow dynamicRow)
    {
        Visit(command,
            [&] (const TWriteRowCommand& command) { store->WriteRow(transaction, dynamicRow, command.Row); },
            [&] (const TDeleteRowCommand&) { store->DeleteRow(transaction, dynamicRow); },
            [&] (const TWriteAndLockRowCommand& command) { store->WriteRow(transaction, dynamicRow, command.Row); },
            [&] (auto) { YT_ABORT(); });
    };

    if (rowRef.Store == ActiveStore_) {
        applyCommand(ActiveStore_, rowRef.Row);
        ActiveStore_->CommitRow(transaction, rowRef.Row, rowRef.LockMask);
    } else {
        auto migratedRow = ActiveStore_->MigrateRow(transaction, rowRef.Row, rowRef.LockMask);
        applyCommand(rowRef.Store, rowRef.Row);
        rowRef.Store->CommitRow(transaction, rowRef.Row, rowRef.LockMask);
        CheckForUnlockedStore(rowRef.Store);
        applyCommand(ActiveStore_, migratedRow);
        ActiveStore_->CommitRow(transaction, migratedRow, rowRef.LockMask);
    }

    return true;
}

void TSortedStoreManager::AbortRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
{
    rowRef.Store->AbortRow(transaction, rowRef.Row, rowRef.LockMask);
    CheckForUnlockedStore(rowRef.Store);
}

IDynamicStore* TSortedStoreManager::GetActiveStore() const
{
    return ActiveStore_.Get();
}

bool TSortedStoreManager::CheckInactiveStoresLocks(
    TUnversionedRow row,
    TLockMask lockMask,
    TWriteContext* context)
{
    auto* transaction = context->Transaction;

    for (const auto& store : LockedStores_) {
        if (!store->AsSortedDynamic()->CheckRowLocks(row, lockMask, context)) {
            return false;
        }
    }

    for (auto it = MaxTimestampToStore_.rbegin();
        it != MaxTimestampToStore_.rend() && it->first > transaction->GetStartTimestamp();
        ++it)
    {
        const auto& store = it->second;
        // Avoid checking locked stores twice.
        if (store->GetType() == EStoreType::SortedDynamic &&
            store->AsSortedDynamic()->GetLockCount() > 0)
        {
            continue;
        }

        if (!store->CheckRowLocks(row, lockMask, context)) {
            return false;
        }
    }

    return true;
}

void TSortedStoreManager::BuildPivotKeys(
    std::vector<TLegacyOwningKey>* pivotKeys,
    const std::vector<TBoundaryDescriptor>& chunkBoundaries)
{
    const std::array<int, 3> depthChange = {-1, 1, -1};

    int depth = 0;
    i64 cumulativeDataSize = 0;
    const auto& mountConfig = Tablet_->GetSettings().MountConfig;
    for (const auto& boundary : chunkBoundaries) {
        if (boundary.Type == 1 &&
            depth == 0 &&
            boundary.Key > Tablet_->GetPivotKey() &&
            cumulativeDataSize >= mountConfig->MinPartitionDataSize)
        {
            pivotKeys->push_back(boundary.Key);
            cumulativeDataSize = 0;
        }
        if (boundary.Type == 1) {
            cumulativeDataSize += boundary.DataSize;
        }
        depth += depthChange[boundary.Type];
    }
}

void TSortedStoreManager::Mount(
    TRange<const TAddStoreDescriptor*> storeDescriptors,
    TRange<const TAddHunkChunkDescriptor*> hunkChunkDescriptors,
    bool createDynamicStore,
    const TMountHint& mountHint)
{
    Tablet_->CreateInitialPartition();

    std::vector<TBoundaryDescriptor> chunkBoundaries;
    int descriptorIndex = 0;
    const auto& schema = *Tablet_->GetPhysicalSchema();
    chunkBoundaries.reserve(storeDescriptors.size());

    auto edenStoreIds = FromProto<THashSet<TStoreId>>(mountHint.eden_store_ids());

    auto isEden = [&] (bool isEdenChunk, TStoreId storeId) {
        // NB: Old tablets may lack eden store ids on master.
        return edenStoreIds.empty()
            ? isEdenChunk
            : edenStoreIds.contains(storeId);
    };

    for (const auto* descriptor : storeDescriptors) {
        const auto& extensions = descriptor->chunk_meta().extensions();
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(extensions);

        if (isEden(miscExt.eden(), FromProto<TStoreId>(descriptor->store_id()))) {
            ++descriptorIndex;
            continue;
        }

        auto boundaryKeysExt = GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(extensions);
        auto minBoundaryKey = WidenKey(FromProto<TLegacyOwningKey>(boundaryKeysExt.min()), schema.GetKeyColumnCount());
        auto maxBoundaryKey = WidenKey(FromProto<TLegacyOwningKey>(boundaryKeysExt.max()), schema.GetKeyColumnCount());

        const auto& chunkViewDescriptor = descriptor->chunk_view_descriptor();

        // Here we use three types.
        // 0 - )
        // 1 - [
        // 2 - ]
        TLegacyOwningKey minKey;
        if (chunkViewDescriptor.read_range().lower_limit().has_legacy_key()) {
            auto chunkViewLimit = FromProto<TLegacyOwningKey>(chunkViewDescriptor.read_range().lower_limit().legacy_key());
            minKey = std::max(chunkViewLimit, minBoundaryKey);
        } else {
            minKey = std::move(minBoundaryKey);
        }

        int maxKeyType;
        TLegacyOwningKey maxKey;
        if (chunkViewDescriptor.read_range().upper_limit().has_legacy_key()) {
            auto chunkViewLimit = FromProto<TLegacyOwningKey>(chunkViewDescriptor.read_range().upper_limit().legacy_key());
            if (chunkViewLimit <= maxBoundaryKey) {
                maxKeyType = 0;
                maxKey = chunkViewLimit;
            } else {
                maxKeyType = 2;
                maxKey = maxBoundaryKey;
            }
        } else {
            maxKeyType = 2;
            maxKey = std::move(maxBoundaryKey);
        }

        chunkBoundaries.push_back({WidenKey(minKey, schema.GetKeyColumnCount()), 1, descriptorIndex, miscExt.compressed_data_size()});
        chunkBoundaries.push_back({WidenKey(maxKey, schema.GetKeyColumnCount()), maxKeyType, descriptorIndex, -1});

        ++descriptorIndex;
    }

    if (!chunkBoundaries.empty()) {
        std::sort(chunkBoundaries.begin(), chunkBoundaries.end(),
            [] (const TBoundaryDescriptor& lhs, const TBoundaryDescriptor& rhs) -> bool {
                return std::tie(lhs.Key, lhs.Type, lhs.DescriptorIndex, lhs.DataSize) <
                       std::tie(rhs.Key, rhs.Type, rhs.DescriptorIndex, rhs.DataSize);
        });

        const auto& mountConfig = Tablet_->GetSettings().MountConfig;
        if (mountConfig->EnableLsmVerboseLogging) {
            YT_LOG_DEBUG("Considering store boundaries during table mount (BoundaryCount: %v)",
                chunkBoundaries.size());
            for (const auto& boundary : chunkBoundaries) {
                YT_LOG_DEBUG("Next chunk boundary (Key: %v, Type: %v, DescriptorIndex: %v, DataSize: %v)",
                    boundary.Key,
                    boundary.Type,
                    boundary.DescriptorIndex,
                    boundary.DataSize);
            }
        }

        std::vector<TLegacyOwningKey> pivotKeys{Tablet_->GetPivotKey()};
        BuildPivotKeys(&pivotKeys, chunkBoundaries);

        YT_VERIFY(Tablet_->PartitionList().size() == 1);
        DoSplitPartition(0, pivotKeys);
    }

    TStoreManagerBase::Mount(
        storeDescriptors,
        hunkChunkDescriptors,
        createDynamicStore,
        mountHint);
}

void TSortedStoreManager::Remount(const NTabletNode::TTableSettings& settings)
{
    int oldSamplesPerPartition = Tablet_->GetSettings().MountConfig->SamplesPerPartition;
    int newSamplesPerPartition = settings.MountConfig->SamplesPerPartition;

    TStoreManagerBase::Remount(settings);

    if (oldSamplesPerPartition != newSamplesPerPartition) {
        SchedulePartitionsSampling(0, Tablet_->PartitionList().size());
    }
}

void TSortedStoreManager::AddStore(IStorePtr store, bool onMount, bool onFlush, TPartitionId partitionIdHint)
{
    TStoreManagerBase::AddStore(store, onMount, onFlush, partitionIdHint);

    auto sortedStore = store->AsSorted();
    MaxTimestampToStore_.emplace(sortedStore->GetMaxTimestamp(), sortedStore);

    SchedulePartitionSampling(sortedStore->GetPartition());
}

void TSortedStoreManager::BulkAddStores(TRange<IStorePtr> stores, bool onMount)
{
    TBulkInsertProfiler bulkInsertProfiler(Tablet_);
    THashMap<TPartitionId, std::vector<ISortedStorePtr>> addedStoresByPartition;
    for (const auto& store : stores) {
        bulkInsertProfiler.Update(store);
        AddStore(store, onMount, /*onFlush*/ false);
        auto sortedStore = store->AsSorted();
        addedStoresByPartition[sortedStore->GetPartition()->GetId()].push_back(sortedStore);
    }

    const auto& Logger = this->Logger;
    const auto& mountConfig = Tablet_->GetSettings().MountConfig;

    for (auto& [partitionId, addedStores] : addedStoresByPartition) {
        if (partitionId == Tablet_->GetEden()->GetId()) {
            continue;
        }

        auto* partition = Tablet_->GetPartition(partitionId);
        YT_LOG_DEBUG_IF(mountConfig->EnableLsmVerboseLogging,
            "Added stores to partition (PartitionId: %v, StoreCount: %v)",
            partition->GetId(),
            addedStores.size());

        TrySplitPartitionByAddedStores(partition, std::move(addedStores));
    }
}

void TSortedStoreManager::DiscardAllStores()
{
    // TODO(ifsmirnov): should flush because someone might want to read from this
    // dynamic store having taken snapshot lock for the table.
    Rotate(/*createNewStore*/ static_cast<bool>(GetActiveStore()), NLsm::EStoreRotationReason::Discard);

    TStoreManagerBase::DiscardAllStores();

    // TODO(ifsmirnov): Reset initial partition. It's non-trivial because partition balancer tasks
    // expect partitions in some states to stay alive long enough, though do not hold references to them.
}

void TSortedStoreManager::RemoveStore(IStorePtr store)
{
    // The range is likely to contain at most one element.
    auto sortedStore = store->AsSorted();
    auto range = MaxTimestampToStore_.equal_range(sortedStore->GetMaxTimestamp());
    for (auto it = range.first; it != range.second; ++it) {
        if (it->second == sortedStore) {
            MaxTimestampToStore_.erase(it);
            break;
        }
    }

    if (sortedStore->IsDynamic()) {
        auto sortedDynamicStore = store->AsSortedDynamic();
        auto flushIndex = sortedDynamicStore->GetFlushIndex();
        if (flushIndex > 0) {
            YT_VERIFY(StoreFlushIndexQueue_.erase(flushIndex) == 1);
        }
    }

    SchedulePartitionSampling(sortedStore->GetPartition());

    TStoreManagerBase::RemoveStore(store);
}

void TSortedStoreManager::CreateActiveStore(TDynamicStoreId hintId)
{
    auto storeId = hintId ? hintId : GenerateDynamicStoreId();

    ActiveStore_ = TabletContext_
        ->CreateStore(Tablet_, EStoreType::SortedDynamic, storeId, nullptr)
        ->AsSortedDynamic();
    ActiveStore_->Initialize();

    ActiveStore_->SetRowBlockedHandler(CreateRowBlockedHandler(ActiveStore_));

    if (Tablet_->GetState() == ETabletState::UnmountFlushing ||
        Tablet_->GetState() == ETabletState::FreezeFlushing)
    {
        ActiveStore_->SetStoreState(EStoreState::PassiveDynamic);
        YT_LOG_INFO(
            "Rotation request received while tablet is in flushing state, "
            "active store created as passive (StoreId: %v, TabletState: %v)",
            storeId,
            Tablet_->GetState());
    } else {
        YT_LOG_INFO("Active store created (StoreId: %v)",
            storeId);
    }

    Tablet_->AddStore(ActiveStore_, /*onFlush*/ false);
    Tablet_->SetActiveStore(ActiveStore_);
}

void TSortedStoreManager::ResetActiveStore()
{
    ActiveStore_.Reset();
}

void TSortedStoreManager::OnActiveStoreRotated()
{
    auto storeFlushIndex = Tablet_->GetStoreFlushIndex();
    ++storeFlushIndex;

    YT_LOG_INFO("Active sorted store rotated (StoreId: %v, StoreFlushIndex: %v)",
        ActiveStore_->GetId(),
        storeFlushIndex);

    Tablet_->SetStoreFlushIndex(storeFlushIndex);
    ActiveStore_->SetFlushIndex(storeFlushIndex);
    YT_VERIFY(StoreFlushIndexQueue_.insert(storeFlushIndex).second);

    MaxTimestampToStore_.emplace(ActiveStore_->GetMaxTimestamp(), ActiveStore_);
}

TStoreFlushCallback TSortedStoreManager::MakeStoreFlushCallback(
    IDynamicStorePtr store,
    TTabletSnapshotPtr tabletSnapshot,
    bool isUnmountWorkflow)
{
    auto sortedDynamicStore = store->AsSortedDynamic();
    auto reader = sortedDynamicStore->CreateFlushReader();
    // NB: Memory store reader is always synchronous.
    YT_VERIFY(reader->Open().Get().IsOK());

    auto inMemoryMode = isUnmountWorkflow ? EInMemoryMode::None : GetInMemoryMode();

    auto storeFlushIndex = sortedDynamicStore->GetFlushIndex();

    return BIND([=, this, this_ = MakeStrong(this)] (
        const ITransactionPtr& transaction,
        const IThroughputThrottlerPtr& throttler,
        TTimestamp currentTimestamp,
        const TWriterProfilerPtr& writerProfiler
    ) {
        const auto& mountConfig = tabletSnapshot->Settings.MountConfig;

        auto workloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletStoreFlush);

        auto storeWriterConfig = CloneYsonStruct(tabletSnapshot->Settings.StoreWriterConfig);
        storeWriterConfig->WorkloadDescriptor = workloadDescriptor;
        storeWriterConfig->MinUploadReplicationFactor = storeWriterConfig->UploadReplicationFactor;
        storeWriterConfig->Postprocess();

        auto storeWriterOptions = CloneYsonStruct(tabletSnapshot->Settings.StoreWriterOptions);
        storeWriterOptions->ChunksEden = true;
        storeWriterOptions->ValidateResourceUsageIncrease = false;
        storeWriterOptions->ConsistentChunkReplicaPlacementHash = tabletSnapshot->ConsistentChunkReplicaPlacementHash;
        storeWriterOptions->Postprocess();

        auto hunkWriterConfig = CloneYsonStruct(tabletSnapshot->Settings.HunkWriterConfig);
        hunkWriterConfig->WorkloadDescriptor = workloadDescriptor;
        hunkWriterConfig->MinUploadReplicationFactor = hunkWriterConfig->UploadReplicationFactor;
        hunkWriterConfig->Postprocess();

        auto hunkWriterOptions = CloneYsonStruct(tabletSnapshot->Settings.HunkWriterOptions);
        hunkWriterOptions->ValidateResourceUsageIncrease = false;
        hunkWriterOptions->ConsistentChunkReplicaPlacementHash = tabletSnapshot->ConsistentChunkReplicaPlacementHash;
        hunkWriterOptions->Postprocess();

        auto asyncBlockCache = CreateRemoteInMemoryBlockCache(
            Client_,
            TabletContext_->GetControlInvoker(),
            TabletContext_->GetLocalDescriptor(),
            TabletContext_->GetLocalRpcServer(),
            Client_->GetNativeConnection()->GetCellDirectory()->GetDescriptorByCellIdOrThrow(tabletSnapshot->CellId),
            inMemoryMode,
            InMemoryManager_->GetConfig());

        auto blockCache = WaitFor(asyncBlockCache)
            .ValueOrThrow();

        auto combinedThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
            throttler,
            tabletSnapshot->FlushThrottler,
        });

        auto tabletCellTag = CellTagFromId(tabletSnapshot->TabletId);

        auto storeChunkWriter = CreateConfirmingWriter(
            storeWriterConfig,
            storeWriterOptions,
            tabletCellTag,
            transaction->GetId(),
            tabletSnapshot->SchemaId,
            /*parentChunkListId*/ {},
            Client_,
            TabletContext_->GetLocalHostName(),
            blockCache,
            /*trafficMeter*/ nullptr,
            combinedThrottler);

        auto hunkChunkWriter = CreateConfirmingWriter(
            hunkWriterConfig,
            hunkWriterOptions,
            tabletCellTag,
            transaction->GetId(),
            tabletSnapshot->SchemaId,
            /*parentChunkListId*/ {},
            Client_,
            TabletContext_->GetLocalHostName(),
            GetNullBlockCache(),
            /*trafficMeter*/ nullptr,
            combinedThrottler);
        auto hunkChunkPayloadWriter = CreateHunkChunkPayloadWriter(
            workloadDescriptor,
            hunkWriterConfig,
            hunkChunkWriter);
        auto hunkChunkWriterStatistics = CreateHunkChunkWriterStatistics(
            tabletSnapshot->Settings.MountConfig->EnableHunkColumnarProfiling,
            tabletSnapshot->PhysicalSchema);
        if (tabletSnapshot->PhysicalSchema->HasHunkColumns()) {
            WaitFor(hunkChunkPayloadWriter->Open())
                .ThrowOnError();
        }

        auto storeWriter = CreateHunkEncodingVersionedWriter(
            CreateVersionedChunkWriter(
                storeWriterConfig,
                storeWriterOptions,
                tabletSnapshot->PhysicalSchema,
                storeChunkWriter,
                /*dataSink*/ std::nullopt,
                blockCache),
            tabletSnapshot->PhysicalSchema,
            hunkChunkPayloadWriter,
            hunkChunkWriterStatistics,
            tabletSnapshot->DictionaryCompressionFactory,
            TClientChunkReadOptions{
                // TODO(akozhikhov): Populate with memory tracker?
                .WorkloadDescriptor = workloadDescriptor,
                .ReadSessionId = TReadSessionId::Create(),
                .HunkChunkReaderStatistics = CreateHunkChunkReaderStatistics(
                    tabletSnapshot->Settings.MountConfig->EnableHunkColumnarProfiling,
                    tabletSnapshot->PhysicalSchema),
            });

        auto updateProfilerGuard = Finally([&] {
            writerProfiler->Update(storeWriter);
            writerProfiler->Update(hunkChunkPayloadWriter, hunkChunkWriterStatistics);
        });

        auto onFlushRowMerger = CreateVersionedRowMerger(
            mountConfig->RowMergerType,
            New<TRowBuffer>(TMergeRowsOnFlushBufferTag()),
            tabletSnapshot->QuerySchema->GetColumnCount(),
            tabletSnapshot->QuerySchema->GetKeyColumnCount(),
            TColumnFilter(),
            mountConfig,
            currentTimestamp,
            MinTimestamp,
            tabletSnapshot->ColumnEvaluator,
            /*lookup*/ false,
            /*mergeRowsOnFlush*/ true,
            /*mergeDeletionsOnFlush*/ mountConfig->MergeDeletionsOnFlush);

        auto unflushedTimestamp = MaxTimestamp;
        auto edenStores = tabletSnapshot->GetEdenStores();
        for (const auto& store : edenStores) {
            if (store->IsDynamic()) {
                unflushedTimestamp = std::min(unflushedTimestamp, store->GetMinTimestamp());
            }
        }

        auto majorTimestamp = std::min(unflushedTimestamp, tabletSnapshot->RetainedTimestamp);

        auto compactionRowMerger = CreateVersionedRowMerger(
            mountConfig->RowMergerType,
            New<TRowBuffer>(TMergeRowsOnFlushBufferTag()),
            tabletSnapshot->QuerySchema->GetColumnCount(),
            tabletSnapshot->QuerySchema->GetKeyColumnCount(),
            TColumnFilter(),
            mountConfig,
            currentTimestamp,
            majorTimestamp,
            tabletSnapshot->ColumnEvaluator,
            /*lookup*/ true, // Forbid null rows. All rows in cache must have a key.
            /*mergeRowsOnFlush*/ false);

        // Retained timestamp according to compactionRowMerger.
        auto newRetainedTimestamp = CalculateRetainedTimestamp(currentTimestamp, mountConfig->MinDataTtl);

        const auto& rowCache = tabletSnapshot->RowCache;

        bool mergeRowsOnFlush = mountConfig->MergeRowsOnFlush &&
            sortedDynamicStore->IsMergeRowsOnFlushAllowed();

        if (mountConfig->MergeRowsOnFlush && !sortedDynamicStore->IsMergeRowsOnFlushAllowed()) {
            YT_LOG_DEBUG("Merge rows on flush is disabled since backup is in progress");
        }

        YT_LOG_DEBUG("Sorted store flush started (StoreId: %v, MergeRowsOnFlush: %v, "
            "MergeDeletionsOnFlush: %v, RetentionConfig: %v, HaveRowCache: %v, "
            "CurrentRetainedTimestamp: %v, NewRetainedTimestamp: %v, StoreFlushIndex: %v)",
            store->GetId(),
            mergeRowsOnFlush,
            mountConfig->MergeDeletionsOnFlush,
            ConvertTo<TRetentionConfigPtr>(mountConfig),
            static_cast<bool>(rowCache),
            tabletSnapshot->RetainedTimestamp,
            newRetainedTimestamp,
            storeFlushIndex);

        // If storeFlushIndex is zero then store is passive dynamic store loaded from snapshot.
        if (rowCache && storeFlushIndex > 0) {
            rowCache->SetFlushIndex(storeFlushIndex);
        }

        auto rowsInStore = 0;
        TUpdateCacheStatistics cacheUpdateStatistics;

        THazardPtrReclaimOnContextSwitchGuard reclaimGuard;

        TRowBatchReadOptions readOptions{
            .MaxRowsPerRead = MaxRowsPerFlushRead
        };

        while (true) {
            // NB: Memory store reader is always synchronous.
            auto batch = reader->Read(readOptions);
            if (!batch || batch->IsEmpty()) {
                break;
            }

            auto range = batch->MaterializeRows();
            std::vector<TVersionedRow> rows(range.begin(), range.end());

            if (mergeRowsOnFlush) {
                auto outputIt = rows.begin();
                for (auto row : rows) {
                    onFlushRowMerger->AddPartialRow(row);
                    auto mergedRow = onFlushRowMerger->BuildMergedRow();
                    if (mergedRow) {
                        *outputIt++ = mergedRow;
                    }
                }
                rows.resize(std::distance(rows.begin(), outputIt));
            }

            rowsInStore += std::ssize(rows);

            if (rowCache && storeFlushIndex > 0) {
                auto statistics = rowCache->UpdateItems(rows, newRetainedTimestamp, compactionRowMerger.get(), storeFlushIndex, Logger);

                cacheUpdateStatistics.FoundRows += statistics.FoundRows;
                cacheUpdateStatistics.DiscardedRows += statistics.DiscardedRows;
                cacheUpdateStatistics.FailedByMemoryRows += statistics.FailedByMemoryRows;
            }

            if (!storeWriter->Write(rows)) {
                WaitFor(storeWriter->GetReadyEvent())
                    .ThrowOnError();
            }

            onFlushRowMerger->Reset();
            compactionRowMerger->Reset();
        }

        if (rowCache) {
            rowCache->ReallocateItems(Logger);
        }

        if (storeWriter->GetRowCount() == 0) {
            YT_LOG_DEBUG("Sorted store is empty, nothing to flush (StoreId: %v)",
                store->GetId());
            return TStoreFlushResult();
        }

        WaitFor(storeWriter->Close())
            .ThrowOnError();
        if (tabletSnapshot->PhysicalSchema->HasHunkColumns()) {
            WaitFor(hunkChunkPayloadWriter->Close())
                .ThrowOnError();
        }

        std::vector<TChunkInfo> chunkInfos{
            TChunkInfo{
                .ChunkId = storeWriter->GetChunkId(),
                .ChunkMeta = storeWriter->GetMeta(),
                .TabletId = tabletSnapshot->TabletId,
                .MountRevision = tabletSnapshot->MountRevision
            }
        };

        WaitFor(blockCache->Finish(chunkInfos))
            .ThrowOnError();

        auto getDiskSpace = [&] (const auto& writer, const auto& writerOptions) {
            auto dataStatistics = writer->GetDataStatistics();
            return CalculateDiskSpaceUsage(
                writerOptions->ReplicationFactor,
                dataStatistics.regular_disk_space(),
                dataStatistics.erasure_disk_space());
        };

        auto totalDiskSpace = getDiskSpace(storeWriter, tabletSnapshot->Settings.StoreWriterOptions) +
            getDiskSpace(hunkChunkWriter, tabletSnapshot->Settings.HunkWriterOptions);
        auto mediumThrottler = GetBlobMediumWriteThrottler(
            TabletContext_->GetDynamicConfigManager(),
            tabletSnapshot);

        YT_LOG_DEBUG("Throttling blobs media write in sorted store flush (DiskSpace: %v)",
            totalDiskSpace);

        WaitFor(mediumThrottler->Throttle(totalDiskSpace))
            .ThrowOnError();

        YT_LOG_DEBUG("Sorted store flushed (StoreId: %v, StoreChunkId: %v, StoreChunkDiskSpace: %v%v, RowsInStore %v, FoundCacheRows: %v, DiscardedCacheRows: %v, FailedByMemoryCacheRows: %v)",
            store->GetId(),
            storeChunkWriter->GetChunkId(),
            getDiskSpace(storeWriter, tabletSnapshot->Settings.StoreWriterOptions),
            MakeFormatterWrapper([&] (auto* builder) {
                if (hunkChunkPayloadWriter->HasHunks()) {
                    builder->AppendFormat(", HunkChunkId: %v, HunkChunkDiskSpace: %v",
                        hunkChunkPayloadWriter->GetChunkId(),
                        getDiskSpace(hunkChunkWriter, tabletSnapshot->Settings.HunkWriterOptions));
                }
            }),
            rowsInStore,
            cacheUpdateStatistics.FoundRows,
            cacheUpdateStatistics.DiscardedRows,
            cacheUpdateStatistics.FailedByMemoryRows);

        TStoreFlushResult result;

        {
            auto& descriptor = result.StoresToAdd.emplace_back();
            descriptor.set_store_type(ToProto<int>(EStoreType::SortedChunk));
            ToProto(descriptor.mutable_store_id(), storeChunkWriter->GetChunkId());
            ToProto(descriptor.mutable_backing_store_id(), store->GetId());
            *descriptor.mutable_chunk_meta() = *storeWriter->GetMeta();
            FilterProtoExtensions(descriptor.mutable_chunk_meta()->mutable_extensions(), GetMasterChunkMetaExtensionTagsFilter());
        }

        if (hunkChunkPayloadWriter->HasHunks()) {
            auto& descriptor = result.HunkChunksToAdd.emplace_back();
            ToProto(descriptor.mutable_chunk_id(), hunkChunkPayloadWriter->GetChunkId());
            *descriptor.mutable_chunk_meta() = *hunkChunkPayloadWriter->GetMeta();
            FilterProtoExtensions(descriptor.mutable_chunk_meta()->mutable_extensions(), GetMasterChunkMetaExtensionTagsFilter());
        }

        if (mountConfig->RegisterChunkReplicasOnStoresUpdate) {
            const auto& chunkReplicaCache = TabletContext_->GetChunkReplicaCache();
            // TODO(kvk1920): Consider using chunk + location instead of chunk + node + medium.
            auto registerReplicas = [&] (const IChunkWriterPtr& chunkWriter) {
                const auto& writtenReplicas = chunkWriter->GetWrittenChunkReplicas();
                TChunkReplicaWithMediumList replicas;
                replicas.reserve(writtenReplicas.size());
                for (auto replica : writtenReplicas) {
                    replicas.push_back(replica);
                }

                chunkReplicaCache->RegisterReplicas(chunkWriter->GetChunkId(), replicas);
            };

            registerReplicas(storeChunkWriter);

            if (hunkChunkPayloadWriter->HasHunks()) {
                registerReplicas(hunkChunkWriter);
            }
        }

        return result;
    });
}

bool TSortedStoreManager::IsFlushNeeded() const
{
    // Unfortunately one cannot rely on IStore::GetRowCount call since
    // the latter is not stable (i.e. may return different values during recovery).
    // But it's always safe to say "yes".
    return true;
}

bool TSortedStoreManager::IsStoreCompactable(IStorePtr store) const
{
    if (store->GetStoreState() != EStoreState::Persistent) {
        return false;
    }

    // NB: Partitioning chunk stores with backing ones may interfere with conflict checking.
    auto sortedChunkStore = store->AsSortedChunk();
    if (sortedChunkStore->GetBackingStore()) {
        return false;
    }

    if (sortedChunkStore->GetCompactionState() != EStoreCompactionState::None) {
        return false;
    }

    return true;
}

bool TSortedStoreManager::IsStoreFlushable(IStorePtr store) const
{
    if (!TStoreManagerBase::IsStoreFlushable(store)) {
        return false;
    }

    // Ensure that stores are being flushed in order.
    auto sortedStore = store->AsSortedDynamic();
    auto flushIndex = sortedStore->GetFlushIndex();
    if (flushIndex == 0) {
        return true;
    }

    YT_VERIFY(!StoreFlushIndexQueue_.empty());
    auto nextFlushIndex = *StoreFlushIndexQueue_.begin();
    YT_VERIFY(flushIndex >= nextFlushIndex);
    return flushIndex == nextFlushIndex;
}

ISortedStoreManagerPtr TSortedStoreManager::AsSorted()
{
    return this;
}

bool TSortedStoreManager::SplitPartition(
    int partitionIndex,
    const std::vector<TLegacyOwningKey>& pivotKeys)
{
    auto* partition = Tablet_->PartitionList()[partitionIndex].get();

    // NB: Set the state back to normal; otherwise if some of the below checks fail, we might get
    // a partition stuck in splitting state forever.
    partition->SetState(EPartitionState::Normal);
    partition->SetAllowedSplitTime(TInstant::Now());

    const auto& mountConfig = Tablet_->GetSettings().MountConfig;
    if (std::ssize(Tablet_->PartitionList()) >= mountConfig->MaxPartitionCount) {
        StructuredLogger_->LogEvent("abort_partition_split")
            .Item("partition_id").Value(partition->GetId())
            .Item("reason").Value("partition_count_limit_exceeded");
        return false;
    }

    DoSplitPartition(partitionIndex, pivotKeys);

    // NB: Initial partition is split into new ones with indexes |[partitionIndex, partitionIndex + pivotKeys.size())|.
    SchedulePartitionsSampling(partitionIndex, partitionIndex + pivotKeys.size());

    return true;
}

void TSortedStoreManager::MergePartitions(
    int firstPartitionIndex,
    int lastPartitionIndex)
{
    for (int index = firstPartitionIndex; index <= lastPartitionIndex; ++index) {
        const auto& partition = Tablet_->PartitionList()[index];
        // See SplitPartition.
        // Currently this code is redundant since there's no escape path below,
        // but we prefer to keep it to make things look symmetric.
        partition->SetState(EPartitionState::Normal);
    }

    DoMergePartitions(firstPartitionIndex, lastPartitionIndex);

    // NB: Initial partitions are merged into a single one with index |firstPartitionIndex|.
    SchedulePartitionsSampling(firstPartitionIndex, firstPartitionIndex + 1);
}

void TSortedStoreManager::UpdatePartitionSampleKeys(
    TPartition* partition,
    const TSharedRange<TLegacyKey>& keys)
{
    YT_VERIFY(keys.Empty() || keys[0] > partition->GetPivotKey());

    auto keyList = New<TSampleKeyList>();
    keyList->Keys = keys;
    partition->SetSampleKeys(keyList);

    const auto* mutationContext = GetCurrentMutationContext();
    partition->SetSamplingTime(mutationContext->GetTimestamp());
}

void TSortedStoreManager::SchedulePartitionSampling(TPartition* partition)
{
    if (!HasMutationContext()) {
        return;
    }

    if (partition->IsEden()) {
        return;
    }

    const auto* mutationContext = GetCurrentMutationContext();
    partition->SetSamplingRequestTime(mutationContext->GetTimestamp());
}

void TSortedStoreManager::SchedulePartitionsSampling(int beginPartitionIndex, int endPartitionIndex)
{
    if (!HasMutationContext()) {
        return;
    }

    const auto* mutationContext = GetCurrentMutationContext();
    for (int index = beginPartitionIndex; index < endPartitionIndex; ++index) {
        Tablet_->PartitionList()[index]->SetSamplingRequestTime(mutationContext->GetTimestamp());
    }
}

void TSortedStoreManager::TrySplitPartitionByAddedStores(
    TPartition* partition,
    std::vector<ISortedStorePtr> addedStores)
{
    std::sort(
        addedStores.begin(),
        addedStores.end(),
        [] (ISortedStorePtr lhs, ISortedStorePtr rhs) {
            auto cmp = CompareRows(lhs->GetMinKey(), rhs->GetMinKey());
            if (cmp != 0) {
                return cmp < 0;
            }
            return lhs->GetId() < rhs->GetId();
        });

    const auto& mountConfig = partition->GetTablet()->GetSettings().MountConfig;

    int formerPartitionStoreCount = static_cast<int>(partition->Stores().size()) - static_cast<int>(addedStores.size());

    std::vector<TLegacyOwningKey> proposedPivots{partition->GetPivotKey()};
    i64 cumulativeDataSize = 0;
    int cumulativeStoreCount = 0;
    TLegacyOwningKey lastKey = MinKey();

    for (int storeIndex = 0; storeIndex < std::ssize(addedStores); ++storeIndex) {
        const auto& store = addedStores[storeIndex];

        if (store->GetMinKey() < lastKey) {
            return;
        }

        i64 dataSize = store->GetCompressedDataSize();

        bool strongEvidence = cumulativeDataSize >= mountConfig->DesiredPartitionDataSize ||
            cumulativeStoreCount >= mountConfig->OverlappingStoreImmediateSplitThreshold;
        bool weakEvidence = cumulativeDataSize + dataSize > mountConfig->MaxPartitionDataSize ||
            cumulativeStoreCount + formerPartitionStoreCount >= mountConfig->OverlappingStoreImmediateSplitThreshold;

        if (strongEvidence || (weakEvidence && cumulativeDataSize >= mountConfig->MinPartitionDataSize)) {
            if (store->GetMinKey() >= partition->GetPivotKey()) {
                proposedPivots.push_back(store->GetMinKey());
                cumulativeDataSize = 0;
                cumulativeStoreCount = 0;
            }
        }

        cumulativeDataSize += dataSize;
        ++cumulativeStoreCount;
        lastKey = store->GetUpperBoundKey();
    }

    if (proposedPivots.size() > 1) {
        YT_LOG_DEBUG("Requesting partition split while adding stores (PartitionId: %v, SplitFactor: %v)",
            partition->GetId(),
            proposedPivots.size());

        partition->RequestImmediateSplit(std::move(proposedPivots));

        Tablet_->GetStructuredLogger()->OnImmediatePartitionSplitRequested(partition);
    }
}

void TSortedStoreManager::DoSplitPartition(int partitionIndex, const std::vector<TLegacyOwningKey>& pivotKeys)
{
    auto backoff = Config_->PartitionSplitMergeBackoffTime;
    Tablet_->SplitPartition(partitionIndex, pivotKeys, backoff);
    if (!IsRecovery()) {
        for (int currentIndex = partitionIndex; currentIndex < partitionIndex + std::ssize(pivotKeys); ++currentIndex) {
            Tablet_->PartitionList()[currentIndex]->StartEpoch();
        }
    }
}

void TSortedStoreManager::DoMergePartitions(int firstPartitionIndex, int lastPartitionIndex)
{
    auto backoff = Config_->PartitionSplitMergeBackoffTime;
    Tablet_->MergePartitions(firstPartitionIndex, lastPartitionIndex, backoff);
    if (!IsRecovery()) {
        Tablet_->PartitionList()[firstPartitionIndex]->StartEpoch();
    }
}

void TSortedStoreManager::StartEpoch(ITabletSlotPtr slot)
{
    TStoreManagerBase::StartEpoch(std::move(slot));

    for (const auto& [storeId, store] : Tablet_->StoreIdMap()) {
        if (store->GetType() == EStoreType::SortedDynamic) {
            auto sortedDynamicStore = store->AsSortedDynamic();
            sortedDynamicStore->SetRowBlockedHandler(CreateRowBlockedHandler(store));
        }
    }
}

void TSortedStoreManager::StopEpoch()
{
    for (const auto& [storeId, store] : Tablet_->StoreIdMap()) {
        if (store->GetType() == EStoreType::SortedDynamic) {
            store->AsSortedDynamic()->ResetRowBlockedHandler();
        }
    }

    TStoreManagerBase::StopEpoch();
}

TSortedDynamicStore::TRowBlockedHandler TSortedStoreManager::CreateRowBlockedHandler(
    const IStorePtr& store)
{
    auto epochInvoker = Tablet_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Read);

    if (!epochInvoker) {
        return TSortedDynamicStore::TRowBlockedHandler();
    }

    return BIND_NO_PROPAGATE(
        &TSortedStoreManager::OnRowBlocked,
        MakeWeak(this),
        Unretained(store.Get()),
        std::move(epochInvoker));
}

void TSortedStoreManager::OnRowBlocked(
    IStore* store,
    IInvokerPtr invoker,
    TSortedDynamicRow row,
    TSortedDynamicStore::TConflictInfo conflictInfo,
    TDuration timeout)
{
    Y_UNUSED(WaitFor(
        BIND(
            &TSortedStoreManager::WaitOnBlockedRow,
            MakeStrong(this),
            MakeStrong(store),
            row,
            conflictInfo,
            timeout)
        .AsyncVia(invoker)
        .Run()));
}

void TSortedStoreManager::WaitOnBlockedRow(
    IStorePtr /*store*/,
    TSortedDynamicRow row,
    TSortedDynamicStore::TConflictInfo conflictInfo,
    TDuration timeout)
{
    const auto& lock = row.BeginLocks(Tablet_->GetPhysicalSchema()->GetKeyColumnCount())[conflictInfo.LockIndex];
    const auto* transaction = lock.PreparedTransaction;

    if (!transaction) {
        return;
    }

    if (lock.PrepareTimestamp.load() >= conflictInfo.CheckingTimestamp) {
        return;
    }

    YT_LOG_DEBUG("Waiting on blocked row (Key: %v, LockIndex: %v, TransactionId: %v, Timeout: %v)",
        RowToKey(*Tablet_->GetPhysicalSchema(), row),
        conflictInfo.LockIndex,
        transaction->GetId(),
        timeout);

    Y_UNUSED(WaitFor(transaction->GetFinished().WithTimeout(timeout)));
}

bool TSortedStoreManager::IsOverflowRotationNeeded() const
{
    if (!IsRotationPossible()) {
        return false;
    }

    const auto& mountConfig = Tablet_->GetSettings().MountConfig;
    auto threshold = mountConfig->DynamicStoreOverflowThreshold;
    if (ActiveStore_->GetMaxDataWeight() >= threshold * mountConfig->MaxDynamicStoreRowDataWeight) {
        return true;
    }

    return TStoreManagerBase::IsOverflowRotationNeeded();
}

TError TSortedStoreManager::CheckOverflow() const
{
    const auto& mountConfig = Tablet_->GetSettings().MountConfig;
    if (ActiveStore_ && ActiveStore_->GetMaxDataWeight() >= mountConfig->MaxDynamicStoreRowDataWeight) {
        return TError("Maximum row data weight limit reached")
            << TErrorAttribute("store_id", ActiveStore_->GetId())
            << TErrorAttribute("key", RowToKey(*Tablet_->GetPhysicalSchema(), ActiveStore_->GetMaxDataWeightWitnessKey()))
            << TErrorAttribute("data_weight", ActiveStore_->GetMaxDataWeight())
            << TErrorAttribute("data_weight_limit", mountConfig->MaxDynamicStoreRowDataWeight);
    }

    return TStoreManagerBase::CheckOverflow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

