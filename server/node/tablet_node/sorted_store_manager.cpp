#include "in_memory_manager.h"
#include "sorted_chunk_store.h"
#include "sorted_dynamic_store.h"
#include "sorted_store_manager.h"
#include "tablet.h"
#include "tablet_profiling.h"
#include "tablet_slot.h"
#include "transaction_manager.h"
#include "automaton.h"

#include <yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/server/lib/tablet_node/config.h>

#include <yt/ytlib/chunk_client/confirming_writer.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/client/table_client/versioned_reader.h>
#include <yt/client/table_client/versioned_row.h>
#include <yt/client/table_client/versioned_writer.h>
#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/row_merger.h>

#include <yt/client/table_client/wire_protocol.h>
#include <yt/client/table_client/proto/wire_protocol.pb.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/client/api/transaction.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/ytalloc/memory_zone.h>

#include <util/generic/cast.h>
#include <yt/core/misc/finally.h>

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
using namespace NYTAlloc;

using NTableClient::TKey;

struct TMergeRowsOnFlushTag
{ };

////////////////////////////////////////////////////////////////////////////////

static const size_t MaxRowsPerFlushRead = 1024;

static const auto BlockedRowWaitQuantum = TDuration::MilliSeconds(100);

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
    , KeyColumnCount_(Tablet_->PhysicalSchema().GetKeyColumnCount())
{
    for (const auto& pair : Tablet_->StoreIdMap()) {
        auto store = pair.second->AsSorted();
        if (store->GetStoreState() != EStoreState::ActiveDynamic) {
            MaxTimestampToStore_.insert(std::make_pair(store->GetMaxTimestamp(), store));
        }
    }

    if (Tablet_->GetActiveStore()) {
        ActiveStore_ = Tablet_->GetActiveStore()->AsSortedDynamic();
    }
}

bool TSortedStoreManager::ExecuteWrites(
    TWireProtocolReader* reader,
    TWriteContext* context)
{
    while (!reader->IsFinished()) {
        TSortedDynamicRowRef rowRef;
        auto readerCheckpoint = reader->GetCurrent();
        auto command = reader->ReadCommand();

        switch (command) {
            case EWireProtocolCommand::WriteRow: {
                auto row = reader->ReadUnversionedRow(false);
                rowRef = ModifyRow(row, ERowModificationType::Write, TLockMask(), context);
                break;
            }

            case EWireProtocolCommand::DeleteRow: {
                auto key = reader->ReadUnversionedRow(false);
                rowRef = ModifyRow(key, ERowModificationType::Delete, TLockMask(), context);
                break;
            }

            case EWireProtocolCommand::VersionedWriteRow: {
                auto row = reader->ReadVersionedRow(Tablet_->PhysicalSchemaData(), false);
                rowRef = ModifyRow(row, context);
                break;
            }

            case EWireProtocolCommand::ReadLockWriteRow: {
                TLockMask locks(reader->ReadLockBitmap());
                auto key = reader->ReadUnversionedRow(false);
                rowRef = ModifyRow(key, ERowModificationType::ReadLockWrite, locks, context);
                break;
            }

            default:
                YT_ABORT();
        }

        if (!rowRef) {
            reader->SetCurrent(readerCheckpoint);
            return false;
        }
    }
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
        case ERowModificationType::ReadLockWrite: {
            if (Tablet_->GetAtomicity() == EAtomicity::None) {
                break;
            }
            const auto& columnIndexToLockIndex = Tablet_->ColumnIndexToLockIndex();

            for (int index = KeyColumnCount_; index < row.GetCount(); ++index) {
                const auto& value = row[index];
                int lockIndex = columnIndexToLockIndex[value.Id];
                lockMask.Set(lockIndex, ELockType::Exclusive);
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
        LockRow(context->Transaction, phase == EWritePhase::Prelock, dynamicRowRef);
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

void TSortedStoreManager::LockRow(TTransaction* transaction, bool prelock, const TSortedDynamicRowRef& rowRef)
{
    if (prelock) {
        transaction->PrelockedRows().push(rowRef);
    } else {
        transaction->LockedRows().push_back(rowRef);
    }
}

void TSortedStoreManager::ConfirmRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
{
    transaction->LockedRows().push_back(rowRef);
}

void TSortedStoreManager::PrepareRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
{
    rowRef.Store->PrepareRow(transaction, rowRef.Row);
}

void TSortedStoreManager::CommitRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
{
    if (rowRef.Store == ActiveStore_) {
        ActiveStore_->CommitRow(transaction, rowRef.Row, rowRef.LockMask);
    } else {
        auto migratedRow = ActiveStore_->MigrateRow(transaction, rowRef.Row, rowRef.LockMask);
        rowRef.Store->CommitRow(transaction, rowRef.Row, rowRef.LockMask);
        CheckForUnlockedStore(rowRef.Store);
        ActiveStore_->CommitRow(transaction, migratedRow, rowRef.LockMask);
    }
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

void TSortedStoreManager::BuildPivotKeysBeforeGiantTabletProblem(
    std::vector<TOwningKey>* pivotKeys,
    const std::vector<TBoundaryDescriptor>& chunkBoundaries)
{
    int depth = 0;
    for (const auto& boundary : chunkBoundaries) {
        if (boundary.Type == -1 && depth == 0 && boundary.Key > Tablet_->GetPivotKey()) {
            pivotKeys->push_back(boundary.Key);
        }
        depth -= boundary.Type;
    }
}

void TSortedStoreManager::BuildPivotKeysBeforeChunkViewsForPivots(
    std::vector<TOwningKey>* pivotKeys,
    const std::vector<TBoundaryDescriptor>& chunkBoundaries)
{
    int depth = 0;
    i64 cumulativeDataSize = 0;
    for (const auto& boundary : chunkBoundaries) {
        if (boundary.Type == -1 &&
            depth == 0 &&
            boundary.Key > Tablet_->GetPivotKey() &&
            cumulativeDataSize >= Tablet_->GetConfig()->MinPartitionDataSize)
        {
            pivotKeys->push_back(boundary.Key);
            cumulativeDataSize = 0;
        }
        if (boundary.Type == -1) {
            cumulativeDataSize += boundary.DataSize;
        }
        depth -= boundary.Type;
    }
}

void TSortedStoreManager::BuildPivotKeys(
    std::vector<TOwningKey>* pivotKeys,
    const std::vector<TBoundaryDescriptor>& chunkBoundaries)
{
    const std::array<int, 3> depthChange = {-1, 1, -1};

    int depth = 0;
    i64 cumulativeDataSize = 0;
    for (const auto& boundary : chunkBoundaries) {
        if (boundary.Type == 1 &&
            depth == 0 &&
            boundary.Key > Tablet_->GetPivotKey() &&
            cumulativeDataSize >= Tablet_->GetConfig()->MinPartitionDataSize)
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

void TSortedStoreManager::Mount(const std::vector<TAddStoreDescriptor>& storeDescriptors)
{
    Tablet_->CreateInitialPartition();

    std::vector<TBoundaryDescriptor> chunkBoundaries;
    int descriptorIndex = 0;
    const auto& schema = Tablet_->PhysicalSchema();
    chunkBoundaries.reserve(storeDescriptors.size());
    for (const auto& descriptor : storeDescriptors) {
        const auto& extensions = descriptor.chunk_meta().extensions();
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(extensions);
        if (miscExt.eden()) {
            ++descriptorIndex;
            continue;
        }

        auto boundaryKeysExt = GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(extensions);
        auto minBoundaryKey = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.min()), schema.GetKeyColumnCount());
        auto maxBoundaryKey = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.max()), schema.GetKeyColumnCount());

        // COMPAT(akozhikhov)
        if (GetCurrentMutationContext()->Request().Reign < ToUnderlying(ETabletReign::ChunkViewsForPivots))  {
            chunkBoundaries.push_back({minBoundaryKey, -1, descriptorIndex, miscExt.compressed_data_size()});
            chunkBoundaries.push_back({maxBoundaryKey, 1, descriptorIndex, miscExt.compressed_data_size()});
            ++descriptorIndex;
        } else {
            const auto& chunkView = descriptor.chunk_view_descriptor();

            // Here we use three types.
            // 0 - )
            // 1 - [
            // 2 - ]
            TOwningKey minKey;
            if (descriptor.has_chunk_view_descriptor()
                && chunkView.has_read_range()
                && chunkView.read_range().has_lower_limit()
                && chunkView.read_range().lower_limit().has_key())
            {
                auto chunkViewLimit = FromProto<TOwningKey>(chunkView.read_range().lower_limit().key());

                // COMPAT(ifsmirnov)
                if (GetCurrentMutationContext()->Request().Reign < ToUnderlying(ETabletReign::ChunkViewWideRange_YT_12532)) {
                    minKey = chunkViewLimit;
                } else {
                    minKey = std::max(chunkViewLimit, minBoundaryKey);
                }
            } else {
                minKey = std::move(minBoundaryKey);
            }

            int maxKeyType;
            TOwningKey maxKey;
            if (descriptor.has_chunk_view_descriptor()
                && chunkView.has_read_range()
                && chunkView.read_range().has_upper_limit()
                && chunkView.read_range().upper_limit().has_key())
            {
                auto chunkViewLimit = FromProto<TOwningKey>(chunkView.read_range().upper_limit().key());

                // COMPAT(ifsmirnov)
                if (GetCurrentMutationContext()->Request().Reign < ToUnderlying(ETabletReign::ChunkViewWideRange_YT_12532)) {
                    maxKeyType = 0;
                    maxKey = chunkViewLimit;
                } else {
                    if (chunkViewLimit <= maxBoundaryKey) {
                        maxKeyType = 0;
                        maxKey = chunkViewLimit;
                    } else {
                        maxKeyType = 2;
                        maxKey = maxBoundaryKey;
                    }
                }
            } else {
                maxKeyType = 2;
                maxKey = std::move(maxBoundaryKey);
            }

            chunkBoundaries.push_back({WidenKey(minKey, schema.GetKeyColumnCount()), 1, descriptorIndex, miscExt.compressed_data_size()});
            chunkBoundaries.push_back({WidenKey(maxKey, schema.GetKeyColumnCount()), maxKeyType, descriptorIndex, -1});

            ++descriptorIndex;
        }
    }

    if (!chunkBoundaries.empty()) {
        std::sort(chunkBoundaries.begin(), chunkBoundaries.end(),
            [] (const TBoundaryDescriptor& lhs, const TBoundaryDescriptor& rhs) -> bool {
                return std::tie(lhs.Key, lhs.Type, lhs.DescriptorIndex, lhs.DataSize) <
                       std::tie(rhs.Key, rhs.Type, rhs.DescriptorIndex, rhs.DataSize);
        });

        if (Tablet_->GetConfig()->EnableLsmVerboseLogging) {
            YT_LOG_DEBUG("Considering store boundaries during table mount (BoundaryCount: %v)",
                chunkBoundaries.size());
            for (const auto& boundary : chunkBoundaries) {
                YT_LOG_DEBUG("Next chunk boundary (Key: %v, Type: %v, DescriptorIndex: %v, DataSize: %v)",
                    boundary.Key, boundary.Type, boundary.DescriptorIndex, boundary.DataSize);
            }
        }

        std::vector<TOwningKey> pivotKeys{Tablet_->GetPivotKey()};

        // COMPAT(akozhikhov)
        if (GetCurrentMutationContext()->Request().Reign < ToUnderlying(ETabletReign::GiantTabletProblem)) {
            BuildPivotKeysBeforeGiantTabletProblem(&pivotKeys, chunkBoundaries);
        } else if (GetCurrentMutationContext()->Request().Reign < ToUnderlying(ETabletReign::ChunkViewsForPivots)) {
            BuildPivotKeysBeforeChunkViewsForPivots(&pivotKeys, chunkBoundaries);
        } else {
            BuildPivotKeys(&pivotKeys, chunkBoundaries);
        }

        YT_VERIFY(Tablet_->PartitionList().size() == 1);
        DoSplitPartition(0, pivotKeys);
    }

    TStoreManagerBase::Mount(storeDescriptors);
}

void TSortedStoreManager::Remount(
    TTableMountConfigPtr mountConfig,
    TTabletChunkReaderConfigPtr readerConfig,
    TTabletChunkWriterConfigPtr writerConfig,
    TTabletWriterOptionsPtr writerOptions)
{
    int oldSamplesPerPartition = Tablet_->GetConfig()->SamplesPerPartition;
    int newSamplesPerPartition = mountConfig->SamplesPerPartition;

    TStoreManagerBase::Remount(
        std::move(mountConfig),
        std::move(readerConfig),
        std::move(writerConfig),
        std::move(writerOptions));

    if (oldSamplesPerPartition != newSamplesPerPartition) {
        SchedulePartitionsSampling(0, Tablet_->PartitionList().size());
    }
}

void TSortedStoreManager::AddStore(IStorePtr store, bool onMount)
{
    TStoreManagerBase::AddStore(store, onMount);

    auto sortedStore = store->AsSorted();
    MaxTimestampToStore_.insert(std::make_pair(sortedStore->GetMaxTimestamp(), sortedStore));

    SchedulePartitionSampling(sortedStore->GetPartition());
}

void TSortedStoreManager::BulkAddStores(TRange<IStorePtr> stores, bool onMount)
{
    THashMap<TPartitionId, std::vector<ISortedStorePtr>> addedStoresByPartition;
    for (const auto& store : stores) {
        AddStore(store, onMount);
        auto sortedStore = store->AsSorted();
        addedStoresByPartition[sortedStore->GetPartition()->GetId()].push_back(sortedStore);
    }

    auto Logger = this->Logger;

    for (auto& [partitionId, addedStores] : addedStoresByPartition) {
        if (partitionId == Tablet_->GetEden()->GetId()) {
            continue;
        }

        auto* partition = Tablet_->GetPartition(partitionId);
        YT_LOG_DEBUG_IF(Tablet_->GetConfig()->EnableLsmVerboseLogging,
            "Added %v stores to partition (PartitionId: %v)",
            partition->GetId(),
            addedStores.size());

        if (partition->GetState() == EPartitionState::Splitting || partition->GetState() == EPartitionState::Merging) {
            YT_LOG_DEBUG_IF(Tablet_->GetConfig()->EnableLsmVerboseLogging,
                "Will not request partition split due to improper partition state (PartitionId: %v, PartitionState: %v)",
                partition->GetId(),
                partition->GetState());
            continue;
        }

        TrySplitPartitionByAddedStores(partition, std::move(addedStores));
    }
}

void TSortedStoreManager::DiscardAllStores()
{
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

    SchedulePartitionSampling(sortedStore->GetPartition());

    TStoreManagerBase::RemoveStore(store);
}

void TSortedStoreManager::CreateActiveStore()
{
    auto storeId = TabletContext_->GenerateId(EObjectType::SortedDynamicTabletStore);
    ActiveStore_ = TabletContext_
        ->CreateStore(Tablet_, EStoreType::SortedDynamic, storeId, nullptr)
        ->AsSortedDynamic();

    ActiveStore_->SetRowBlockedHandler(CreateRowBlockedHandler(ActiveStore_));

    Tablet_->AddStore(ActiveStore_);
    Tablet_->SetActiveStore(ActiveStore_);

    YT_LOG_INFO_UNLESS(IsRecovery(), "Active store created (StoreId: %v)",
        storeId);
}

void TSortedStoreManager::ResetActiveStore()
{
    ActiveStore_.Reset();
}

void TSortedStoreManager::OnActiveStoreRotated()
{
    MaxTimestampToStore_.insert(std::make_pair(ActiveStore_->GetMaxTimestamp(), ActiveStore_));
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

    return BIND([=, this_ = MakeStrong(this)] (
        ITransactionPtr transaction,
        IThroughputThrottlerPtr throttler,
        TTimestamp currentTimestamp,
        TWriterProfilerPtr writerProfiler
    ) {
        IVersionedChunkWriterPtr tableWriter;

        auto updateProfilerGuard = Finally([&] () {
            writerProfiler->Update(tableWriter);
        });

        TMemoryZoneGuard memoryZoneGuard(inMemoryMode == EInMemoryMode::None
            ? EMemoryZone::Normal
            : EMemoryZone::Undumpable);

        auto writerOptions = CloneYsonSerializable(tabletSnapshot->WriterOptions);
        writerOptions->ChunksEden = true;
        writerOptions->ValidateResourceUsageIncrease = false;
        auto writerConfig = CloneYsonSerializable(tabletSnapshot->WriterConfig);
        writerConfig->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletStoreFlush);

        auto asyncBlockCache = CreateRemoteInMemoryBlockCache(
            Client_,
            TabletContext_->GetLocalRpcServer(),
            Client_->GetNativeConnection()->GetCellDirectory()->GetDescriptorOrThrow(tabletSnapshot->CellId),
            inMemoryMode,
            InMemoryManager_->GetConfig());

        auto blockCache = WaitFor(asyncBlockCache)
            .ValueOrThrow();

        throttler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
            std::move(throttler),
            tabletSnapshot->FlushThrottler});

        auto chunkWriter = CreateConfirmingWriter(
            writerConfig,
            writerOptions,
            CellTagFromId(tabletSnapshot->TabletId),
            transaction->GetId(),
            NullChunkListId,
            New<TNodeDirectory>(),
            Client_,
            blockCache,
            nullptr,
            std::move(throttler));

        tableWriter = CreateVersionedChunkWriter(
            writerConfig,
            writerOptions,
            tabletSnapshot->PhysicalSchema,
            chunkWriter,
            blockCache);

        TVersionedRowMerger rowMerger(
            New<TRowBuffer>(TMergeRowsOnFlushTag()),
            tabletSnapshot->QuerySchema.GetColumnCount(),
            tabletSnapshot->QuerySchema.GetKeyColumnCount(),
            TColumnFilter(),
            tabletSnapshot->Config,
            currentTimestamp,
            0,
            tabletSnapshot->ColumnEvaluator,
            false,
            true);

        std::vector<TVersionedRow> rows;
        rows.reserve(MaxRowsPerFlushRead);

        YT_LOG_DEBUG("Sorted store flush started (StoreId: %v, MergeRowsOnFlush: %v, RetentionConfig: %v)",
            store->GetId(),
            tabletSnapshot->Config->MergeRowsOnFlush,
            ConvertTo<TRetentionConfigPtr>(tabletSnapshot->Config));

        const auto& rowCache = tabletSnapshot->RowCache;

	THazardPtrFlushGuard flushGuard;

        while (true) {
            // NB: Memory store reader is always synchronous.
            reader->Read(&rows);
            if (rows.empty()) {
                break;
            }

            if (tabletSnapshot->Config->MergeRowsOnFlush) {
                auto outputIt = rows.begin();
                for (auto& row : rows) {
                    rowMerger.AddPartialRow(row);
                    auto mergedRow = rowMerger.BuildMergedRow();
                    if (mergedRow) {
                        *outputIt++ = mergedRow;
                    }
                }
                rows.resize(std::distance(rows.begin(), outputIt));
            }

            if (rowCache) {
                auto accessor = rowCache->Cache.GetLookupAccessor();

                for (auto& row : rows) {
                    // TODO(lukyan): Get here address of cell and use it in update. Or use Update with callback.
                    auto found = accessor.Lookup(row);

                    if (found) {
                        rowMerger.AddPartialRow(found->GetVersionedRow());
                        rowMerger.AddPartialRow(row);

                        row = rowMerger.BuildMergedRow();

                        auto cachedRow = CachedRowFromVersionedRow(
                            &rowCache->Allocator,
                            row);

                        bool updated = accessor.Update(cachedRow);

                        if (updated) {
                            YT_LOG_TRACE("Cache updated (Row: %v)", cachedRow->GetVersionedRow());
                        } else {
                            YT_LOG_TRACE("Cache update failed (Row: %v)", cachedRow->GetVersionedRow());
                        }
                    }
                }
            }

            if (!tableWriter->Write(rows)) {
                WaitFor(tableWriter->GetReadyEvent())
                    .ThrowOnError();
            }
            rowMerger.Reset();
        }

        if (tableWriter->GetRowCount() == 0) {
            return std::vector<TAddStoreDescriptor>();
        }

        WaitFor(tableWriter->Close())
            .ThrowOnError();

        std::vector<TChunkInfo> chunkInfos;
        chunkInfos.emplace_back(
            tableWriter->GetChunkId(),
            tableWriter->GetNodeMeta(),
            tabletSnapshot->TabletId);

        WaitFor(blockCache->Finish(chunkInfos))
            .ThrowOnError();

        auto dataStatistics = tableWriter->GetDataStatistics();
        auto diskSpace = CalculateDiskSpaceUsage(
            tabletSnapshot->WriterOptions->ReplicationFactor,
            dataStatistics.regular_disk_space(),
            dataStatistics.erasure_disk_space());
        YT_LOG_DEBUG("Flushed sorted store (StoreId: %v, ChunkId: %v, DiskSpace: %v)",
            store->GetId(),
            chunkWriter->GetChunkId(),
            diskSpace);

        TAddStoreDescriptor descriptor;
        descriptor.set_store_type(static_cast<int>(EStoreType::SortedChunk));
        ToProto(descriptor.mutable_store_id(), chunkWriter->GetChunkId());
        descriptor.mutable_chunk_meta()->CopyFrom(tableWriter->GetMasterMeta());
        ToProto(descriptor.mutable_backing_store_id(), store->GetId());
        return std::vector<TAddStoreDescriptor>{descriptor};
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
    if (sortedChunkStore->HasBackingStore()) {
        return false;
    }

    if (sortedChunkStore->GetCompactionState() != EStoreCompactionState::None) {
        return false;
    }

    if (!sortedChunkStore->IsCompactionAllowed()) {
        return false;
    }

    return true;
}

ISortedStoreManagerPtr TSortedStoreManager::AsSorted()
{
    return this;
}

bool TSortedStoreManager::SplitPartition(
    int partitionIndex,
    const std::vector<TOwningKey>& pivotKeys)
{
    auto* partition = Tablet_->PartitionList()[partitionIndex].get();

    // NB: Set the state back to normal; otherwise if some of the below checks fail, we might get
    // a partition stuck in splitting state forever.
    partition->SetState(EPartitionState::Normal);
    partition->SetAllowedSplitTime(TInstant::Now());

    if (Tablet_->PartitionList().size() >= Tablet_->GetConfig()->MaxPartitionCount) {
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
    const TSharedRange<TKey>& keys)
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

    const auto& config = partition->GetTablet()->GetConfig();

    int formerPartitionStoreCount = partition->Stores().size() - addedStores.size();

    std::vector<TOwningKey> proposedPivots{partition->GetPivotKey()};
    i64 cumulativeDataSize = 0;
    int cumulativeStoreCount = 0;
    TOwningKey lastKey = partition->GetPivotKey();

    for (int storeIndex = 0; storeIndex < addedStores.size(); ++storeIndex) {
        const auto& store = addedStores[storeIndex];

        if (store->GetMinKey() < lastKey) {
            return;
        }

        i64 dataSize = store->GetCompressedDataSize();

        bool strongEvidence = cumulativeDataSize >= config->DesiredPartitionDataSize ||
            cumulativeStoreCount >= config->OverlappingStoreImmediateSplitThreshold;
        bool weakEvidence = cumulativeDataSize + dataSize > config->MaxPartitionDataSize ||
            cumulativeStoreCount + formerPartitionStoreCount >= config->OverlappingStoreImmediateSplitThreshold;

        if (strongEvidence || (weakEvidence && cumulativeDataSize >= config->MinPartitionDataSize)) {
            proposedPivots.push_back(store->GetMinKey());
            cumulativeDataSize = 0;
            cumulativeStoreCount = 0;
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
    }
}

void TSortedStoreManager::DoSplitPartition(int partitionIndex, const std::vector<TOwningKey>& pivotKeys)
{
    Tablet_->SplitPartition(partitionIndex, pivotKeys);
    if (!IsRecovery()) {
        for (int currentIndex = partitionIndex; currentIndex < partitionIndex + pivotKeys.size(); ++currentIndex) {
            Tablet_->PartitionList()[currentIndex]->StartEpoch();
        }
    }
}

void TSortedStoreManager::DoMergePartitions(int firstPartitionIndex, int lastPartitionIndex)
{
    Tablet_->MergePartitions(firstPartitionIndex, lastPartitionIndex);
    if (!IsRecovery()) {
        Tablet_->PartitionList()[firstPartitionIndex]->StartEpoch();
    }
}

void TSortedStoreManager::StartEpoch(TTabletSlotPtr slot)
{
    TStoreManagerBase::StartEpoch(std::move(slot));

    for (const auto& pair : Tablet_->StoreIdMap()) {
        const auto& store = pair.second;
        if (store->GetType() == EStoreType::SortedDynamic) {
            auto sortedDynamicStore = store->AsSortedDynamic();
            sortedDynamicStore->SetRowBlockedHandler(CreateRowBlockedHandler(store));
        }
    }
}

void TSortedStoreManager::StopEpoch()
{
    for (const auto& pair : Tablet_->StoreIdMap()) {
        const auto& store = pair.second;
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

    return BIND(
        &TSortedStoreManager::OnRowBlocked,
        MakeWeak(this),
        Unretained(store.Get()),
        std::move(epochInvoker));
}

void TSortedStoreManager::OnRowBlocked(
    IStore* store,
    IInvokerPtr invoker,
    TSortedDynamicRow row,
    int lockIndex)
{
    Y_UNUSED(WaitFor(
        BIND(
            &TSortedStoreManager::WaitOnBlockedRow,
            MakeStrong(this),
            MakeStrong(store),
            row,
            lockIndex)
        .AsyncVia(invoker)
        .Run()));
}

void TSortedStoreManager::WaitOnBlockedRow(
    IStorePtr /*store*/,
    TSortedDynamicRow row,
    int lockIndex)
{
    const auto& lock = row.BeginLocks(Tablet_->PhysicalSchema().GetKeyColumnCount())[lockIndex];
    const auto* transaction = lock.WriteTransaction;
    if (!transaction) {
        return;
    }

    YT_LOG_DEBUG("Waiting on blocked row (Key: %v, LockIndex: %v, TransactionId: %v)",
        RowToKey(Tablet_->PhysicalSchema(), row),
        lockIndex,
        transaction->GetId());

    Y_UNUSED(WaitFor(transaction->GetFinished().WithTimeout(BlockedRowWaitQuantum)));
}

bool TSortedStoreManager::IsOverflowRotationNeeded() const
{
    if (!IsRotationPossible()) {
        return false;
    }

    const auto& config = Tablet_->GetConfig();
    auto threshold = config->DynamicStoreOverflowThreshold;
    if (ActiveStore_->GetMaxDataWeight() >= threshold * config->MaxDynamicStoreRowDataWeight) {
        return true;
    }

    return TStoreManagerBase::IsOverflowRotationNeeded();
}

TError TSortedStoreManager::CheckOverflow() const
{
    const auto& config = Tablet_->GetConfig();
    if (ActiveStore_ && ActiveStore_->GetMaxDataWeight() >= config->MaxDynamicStoreRowDataWeight) {
        return TError("Maximum row data weight limit reached")
            << TErrorAttribute("store_id", ActiveStore_->GetId())
            << TErrorAttribute("key", RowToKey(Tablet_->PhysicalSchema(), ActiveStore_->GetMaxDataWeightWitnessKey()))
            << TErrorAttribute("data_weight", ActiveStore_->GetMaxDataWeight())
            << TErrorAttribute("data_weight_limit", config->MaxDynamicStoreRowDataWeight);
    }

    return TStoreManagerBase::CheckOverflow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

