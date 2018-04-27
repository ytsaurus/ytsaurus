#include "config.h"
#include "in_memory_chunk_writer.h"
#include "in_memory_manager.h"
#include "sorted_chunk_store.h"
#include "sorted_dynamic_store.h"
#include "sorted_store_manager.h"
#include "tablet.h"
#include "tablet_profiling.h"
#include "tablet_slot.h"
#include "transaction_manager.h"

#include <yt/server/tablet_node/tablet_manager.pb.h>

#include <yt/ytlib/chunk_client/confirming_writer.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/ytlib/table_client/versioned_reader.h>
#include <yt/ytlib/table_client/versioned_row.h>
#include <yt/ytlib/table_client/versioned_writer.h>
#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/row_merger.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>
#include <yt/ytlib/tablet_client/wire_protocol.pb.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/transaction.h>

namespace NYT {
namespace NTabletNode {

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
    TInMemoryManagerPtr inMemoryManager,
    INativeClientPtr client)
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
                rowRef = ModifyRow(row, ERowModificationType::Write, context);
                break;
            }

            case EWireProtocolCommand::DeleteRow: {
                auto key = reader->ReadUnversionedRow(false);
                rowRef = ModifyRow(key, ERowModificationType::Delete, context);
                break;
            }

            case EWireProtocolCommand::VersionedWriteRow: {
                auto row = reader->ReadVersionedRow(Tablet_->PhysicalSchemaData(), false);
                rowRef = ModifyRow(row, context);
                break;
            }

            default:
                Y_UNREACHABLE();
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
    TWriteContext* context)
{
    auto phase = context->Phase;
    auto atomic = Tablet_->GetAtomicity() == EAtomicity::Full;

    ui32 lockMask = modificationType == ERowModificationType::Write
        ? ComputeLockMask(row)
        : TSortedDynamicRow::PrimaryLockMask;

    if (atomic &&
        phase == EWritePhase::Prelock &&
        !CheckInactiveStoresLocks(row, lockMask, context))
    {
        return TSortedDynamicRowRef();
    }

    if (!atomic) {
        Y_ASSERT(phase == EWritePhase::Commit);
        context->CommitTimestamp = GenerateMonotonicCommitTimestamp(context->CommitTimestamp);
    }

    auto dynamicRow = ActiveStore_->ModifyRow(row, lockMask, modificationType, context);
    if (!dynamicRow) {
        return TSortedDynamicRowRef();
    }

    auto dynamicRowRef = TSortedDynamicRowRef(ActiveStore_.Get(), this, dynamicRow);

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
        ActiveStore_->CommitRow(transaction, rowRef.Row);
    } else {
        auto migratedRow = ActiveStore_->MigrateRow(transaction, rowRef.Row);
        rowRef.Store->CommitRow(transaction, rowRef.Row);
        CheckForUnlockedStore(rowRef.Store);
        ActiveStore_->CommitRow(transaction, migratedRow);
    }
}

void TSortedStoreManager::AbortRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
{
    rowRef.Store->AbortRow(transaction, rowRef.Row);
    CheckForUnlockedStore(rowRef.Store);
}

IDynamicStore* TSortedStoreManager::GetActiveStore() const
{
    return ActiveStore_.Get();
}

ui32 TSortedStoreManager::ComputeLockMask(TUnversionedRow row)
{
    if (Tablet_->GetAtomicity() == EAtomicity::None) {
        return 0;
    }
    const auto& columnIndexToLockIndex = Tablet_->ColumnIndexToLockIndex();
    ui32 lockMask = 0;
    for (int index = KeyColumnCount_; index < row.GetCount(); ++index) {
        const auto& value = row[index];
        int lockIndex = columnIndexToLockIndex[value.Id];
        lockMask |= (1 << lockIndex);
    }
    Y_ASSERT(lockMask != 0);
    return lockMask;
}

bool TSortedStoreManager::CheckInactiveStoresLocks(
    TUnversionedRow row,
    ui32 lockMask,
    TWriteContext* context)
{
    auto* transaction = context->Transaction;

    for (const auto& store : LockedStores_) {
        auto error = store->AsSortedDynamic()->CheckRowLocks(row, transaction, lockMask);
        if (!error.IsOK()) {
            context->Error = error;
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

        auto error = store->CheckRowLocks(row, transaction, lockMask);
        if (!error.IsOK()) {
            context->Error = error;
            return false;
        }
    }

    return true;
}

void TSortedStoreManager::Mount(const std::vector<TAddStoreDescriptor>& storeDescriptors)
{
    Tablet_->CreateInitialPartition();

    std::vector<std::tuple<TOwningKey, int, int>> chunkBoundaries;
    int descriptorIndex = 0;
    const auto& schema = Tablet_->PhysicalSchema();
    for (const auto& descriptor : storeDescriptors) {
        const auto& extensions = descriptor.chunk_meta().extensions();
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(extensions);
        if (!miscExt.eden()) {
            auto boundaryKeysExt = GetProtoExtension<NTableClient::NProto::TBoundaryKeysExt>(extensions);
            auto minKey = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.min()), schema.GetKeyColumnCount());
            auto maxKey = WidenKey(FromProto<TOwningKey>(boundaryKeysExt.max()), schema.GetKeyColumnCount());
            chunkBoundaries.push_back(std::make_tuple(minKey, -1, descriptorIndex));
            chunkBoundaries.push_back(std::make_tuple(maxKey, 1, descriptorIndex));
        }
        ++descriptorIndex;
    }

    if (!chunkBoundaries.empty()) {
        std::sort(chunkBoundaries.begin(), chunkBoundaries.end());
        std::vector<TOwningKey> pivotKeys{Tablet_->GetPivotKey()};
        int depth = 0;
        for (const auto& boundary : chunkBoundaries) {
            if (std::get<1>(boundary) == -1 && depth == 0 && std::get<0>(boundary) > Tablet_->GetPivotKey()) {
                pivotKeys.push_back(std::get<0>(boundary));
            }
            depth -= std::get<1>(boundary);
        }

        YCHECK(Tablet_->PartitionList().size() == 1);
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

    LOG_INFO_UNLESS(IsRecovery(), "Active store created (StoreId: %v)",
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
    TTabletSnapshotPtr tabletSnapshot)
{
    auto sortedDynamicStore = store->AsSortedDynamic();
    auto reader = sortedDynamicStore->CreateFlushReader();
    // NB: Memory store reader is always synchronous.
    YCHECK(reader->Open().Get().IsOK());

    auto inMemoryMode = GetInMemoryMode();
    auto inMemoryConfigRevision = GetInMemoryConfigRevision();

    return BIND([=, this_ = MakeStrong(this)] (ITransactionPtr transaction) {
        auto writerOptions = CloneYsonSerializable(tabletSnapshot->WriterOptions);
        writerOptions->ChunksEden = true;
        writerOptions->ValidateResourceUsageIncrease = false;
        auto writerConfig = CloneYsonSerializable(tabletSnapshot->WriterConfig);
        writerConfig->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletStoreFlush);

        auto blockCache = InMemoryManager_->CreateInterceptingBlockCache(inMemoryMode, inMemoryConfigRevision);

        auto chunkWriter = CreateConfirmingWriter(
            writerConfig,
            writerOptions,
            Client_->GetNativeConnection()->GetPrimaryMasterCellTag(),
            transaction->GetId(),
            NullChunkListId,
            New<TNodeDirectory>(),
            Client_,
            blockCache);

        auto tableWriter = CreateInMemoryVersionedChunkWriter(
            writerConfig,
            writerOptions,
            InMemoryManager_,
            tabletSnapshot,
            chunkWriter,
            blockCache);

        WaitFor(tableWriter->Open())
            .ThrowOnError();

        TVersionedRowMerger rowMerger(
            New<TRowBuffer>(TMergeRowsOnFlushTag()),
            tabletSnapshot->QuerySchema.GetColumnCount(),
            tabletSnapshot->QuerySchema.GetKeyColumnCount(),
            TColumnFilter(),
            tabletSnapshot->Config,
            transaction->GetStartTimestamp(),
            0,
            tabletSnapshot->ColumnEvaluator,
            false,
            true);

        std::vector<TVersionedRow> rows;
        rows.reserve(MaxRowsPerFlushRead);

        LOG_DEBUG("Sorted store flush started (StoreId: %v, MergeRowsOnFlush: %v, RetentionConfig: %Qv)",
            store->GetId(),
            tabletSnapshot->Config->MergeRowsOnFlush,
            ConvertTo<TRetentionConfigPtr>(tabletSnapshot->Config));

        while (true) {
            // NB: Memory store reader is always synchronous.
            reader->Read(&rows);
            if (rows.empty()) {
                break;
            }

            if (tabletSnapshot->Config->MergeRowsOnFlush) {
                for (auto& row : rows) {
                    rowMerger.AddPartialRow(row);
                    row = rowMerger.BuildMergedRow();
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

        ProfileChunkWriter(
            tabletSnapshot,
            tableWriter->GetDataStatistics(),
            tableWriter->GetCompressionStatistics(),
            StoreFlushTag_);

        auto dataStatistics = tableWriter->GetDataStatistics();
        auto diskSpace = CalculateDiskSpaceUsage(
            tabletSnapshot->WriterOptions->ReplicationFactor,
            dataStatistics.regular_disk_space(),
            dataStatistics.erasure_disk_space());
        LOG_DEBUG("Flushed sorted store (StoreId: %v, ChunkId: %v DiskSpace: %v, MergeRowsOnFlush: v)",
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
    YCHECK(keys.Empty() || keys[0] > partition->GetPivotKey());

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
    const auto* transaction = lock.Transaction;
    if (!transaction) {
        return;
    }

    LOG_DEBUG("Waiting on blocked row (Key: %v, LockIndex: %v, TransactionId: %v)",
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

} // namespace NTabletNode
} // namespace NYT

