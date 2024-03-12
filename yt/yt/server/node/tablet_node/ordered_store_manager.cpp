#include "in_memory_manager.h"
#include "ordered_dynamic_store.h"
#include "ordered_store_manager.h"
#include "store.h"
#include "tablet.h"
#include "tablet_profiling.h"
#include "transaction.h"

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>
#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/chunk_client/confirming_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/table_client/versioned_chunk_writer.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/finally.h>

namespace NYT::NTabletNode {

using namespace NConcurrency;
using namespace NApi;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;
using namespace NObjectClient;
using namespace NTransactionClient;

using NTabletNode::NProto::TAddStoreDescriptor;
using NTabletNode::NProto::TMountHint;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const size_t MaxRowsPerFlushRead = 1024;

////////////////////////////////////////////////////////////////////////////////

TOrderedStoreManager::TOrderedStoreManager(
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
{
    if (Tablet_->GetActiveStore()) {
        ActiveStore_ = Tablet_->GetActiveStore()->AsOrderedDynamic();
    }
}

void TOrderedStoreManager::Mount(
    TRange<const NTabletNode::NProto::TAddStoreDescriptor*> storeDescriptors,
    TRange<const NTabletNode::NProto::TAddHunkChunkDescriptor*> hunkChunkDescriptors,
    bool createDynamicStore,
    const TMountHint& mountHint)
{
    TStoreManagerBase::Mount(
        storeDescriptors,
        hunkChunkDescriptors,
        createDynamicStore,
        mountHint);
    Tablet_->UpdateTotalRowCount();
    Tablet_->RecomputeReplicaStatuses();
}

void TOrderedStoreManager::LockHunkStores(TWriteContext* context)
{
    if (context->HunkChunksInfo) {
        ActiveStore_->LockHunkStores(*context->HunkChunksInfo);
    }
}

bool TOrderedStoreManager::ExecuteWrites(
    IWireProtocolReader* reader,
    TWriteContext* context)
{
    YT_VERIFY(context->Phase == EWritePhase::Commit);

    LockHunkStores(context);

    while (!reader->IsFinished()) {
        auto command = reader->ReadWriteCommand(
            Tablet_->TableSchemaData(),
            /*captureValues*/ false,
            /*versionedWriteIsUnversioned*/ true);
        Visit(command,
            [&] (const TWriteRowCommand& command) { WriteRow(command.Row, context); },
            [&] (const TVersionedWriteRowCommand& command) { WriteRow(command.UnversionedRow, context); },
            [&] (const auto& command) {
                THROW_ERROR_EXCEPTION("Unsupported write command %v",
                    GetWireProtocolCommand(command));
            });
    }
    return true;
}

TOrderedDynamicRowRef TOrderedStoreManager::WriteRow(
    TUnversionedRow row,
    TWriteContext* context)
{
    auto dynamicRow = ActiveStore_->WriteRow(row, context);
    return TOrderedDynamicRowRef(
        ActiveStore_.Get(),
        this,
        dynamicRow);
}

i64 TOrderedStoreManager::ComputeStartingRowIndex() const
{
    const auto& storeRowIndexMap = Tablet_->StoreRowIndexMap();
    if (storeRowIndexMap.empty()) {
        return Tablet_->GetTrimmedRowCount();
    }

    const auto& lastStore = storeRowIndexMap.rbegin()->second;
    YT_VERIFY(lastStore->GetRowCount() > 0);
    return lastStore->GetStartingRowIndex() + lastStore->GetRowCount();
}

void TOrderedStoreManager::DiscardAllStores()
{
    YT_ABORT();
}

void TOrderedStoreManager::CreateActiveStore(TDynamicStoreId hintId)
{
    auto storeId = hintId ? hintId : GenerateDynamicStoreId();

    ActiveStore_ = TabletContext_
        ->CreateStore(Tablet_, EStoreType::OrderedDynamic, storeId, nullptr)
        ->AsOrderedDynamic();
    ActiveStore_->Initialize();

    auto startingRowIndex = ComputeStartingRowIndex();
    ActiveStore_->SetStartingRowIndex(startingRowIndex);

    Tablet_->AddStore(ActiveStore_, /*onFlush*/ false);
    Tablet_->SetActiveStore(ActiveStore_);

    if (Tablet_->GetState() == ETabletState::UnmountFlushing ||
        Tablet_->GetState() == ETabletState::FreezeFlushing)
    {
        ActiveStore_->SetStoreState(EStoreState::PassiveDynamic);
        YT_LOG_INFO(
            "Rotation request received while tablet is in flushing state, "
            "active store created as passive (StoreId: %v, StartingRowIndex: %v, TabletState: %v)",
            storeId,
            startingRowIndex,
            Tablet_->GetState());

    } else {
        YT_LOG_INFO("Active store created (StoreId: %v, StartingRowIndex: %v)",
            storeId,
            startingRowIndex);
    }
}

void TOrderedStoreManager::ResetActiveStore()
{
    ActiveStore_.Reset();
}

void TOrderedStoreManager::OnActiveStoreRotated()
{ }

bool TOrderedStoreManager::IsFlushNeeded() const
{
    return ActiveStore_->GetRowCount() > 0;
}

bool TOrderedStoreManager::IsStoreCompactable(IStorePtr /*store*/) const
{
    return false;
}

bool TOrderedStoreManager::IsStoreFlushable(IStorePtr store) const
{
    if (!TStoreManagerBase::IsStoreFlushable(store)) {
        return false;
    }

    // Ensure that stores are being flushed in order.
    auto orderedStore = store->AsOrdered();
    i64 startingRowIndex = orderedStore->GetStartingRowIndex();
    const auto& rowIndexMap = store->GetTablet()->StoreRowIndexMap();
    auto it = rowIndexMap.find(startingRowIndex);
    YT_VERIFY(it != rowIndexMap.end());
    if (it != rowIndexMap.begin() && (--it)->second->GetStoreState() != EStoreState::Persistent) {
        return false;
    }

    return true;
}

IOrderedStoreManagerPtr TOrderedStoreManager::AsOrdered()
{
    return this;
}

IDynamicStore* TOrderedStoreManager::GetActiveStore() const
{
    return ActiveStore_.Get();
}

TStoreFlushCallback TOrderedStoreManager::MakeStoreFlushCallback(
    IDynamicStorePtr store,
    TTabletSnapshotPtr tabletSnapshot,
    bool isUnmountWorkflow)
{
    auto orderedDynamicStore = store->AsOrderedDynamic();
    auto reader = orderedDynamicStore->CreateFlushReader();
    auto inMemoryMode = isUnmountWorkflow ? EInMemoryMode::None : GetInMemoryMode();

    return BIND([=, this, this_ = MakeStrong(this)] (
        const ITransactionPtr& transaction,
        const IThroughputThrottlerPtr& throttler,
        TTimestamp /*currentTimestamp*/,
        const TWriterProfilerPtr& writerProfiler
    ) {
        ISchemalessChunkWriterPtr tableWriter;

        auto updateProfilerGuard = Finally([&] () {
            writerProfiler->Update(tableWriter);
        });

        auto writerOptions = CloneYsonStruct(tabletSnapshot->Settings.StoreWriterOptions);
        writerOptions->ValidateResourceUsageIncrease = false;
        writerOptions->ConsistentChunkReplicaPlacementHash = tabletSnapshot->ConsistentChunkReplicaPlacementHash;
        writerOptions->Postprocess();

        auto writerConfig = CloneYsonStruct(tabletSnapshot->Settings.StoreWriterConfig);
        writerConfig->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletStoreFlush);
        writerConfig->MinUploadReplicationFactor = writerConfig->UploadReplicationFactor;
        writerConfig->Postprocess();

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
            tabletSnapshot->FlushThrottler
        });

        auto tabletCellTag = CellTagFromId(tabletSnapshot->TabletId);
        auto chunkWriter = CreateConfirmingWriter(
            writerConfig,
            writerOptions,
            tabletCellTag,
            transaction->GetId(),
            tabletSnapshot->SchemaId,
            NullChunkListId,
            Client_,
            TabletContext_->GetLocalHostName(),
            blockCache,
            nullptr,
            std::move(combinedThrottler));

        TChunkTimestamps chunkTimestamps;
        chunkTimestamps.MinTimestamp = orderedDynamicStore->GetMinTimestamp();
        chunkTimestamps.MaxTimestamp = orderedDynamicStore->GetMaxTimestamp();

        tableWriter = CreateSchemalessChunkWriter(
            tabletSnapshot->Settings.StoreWriterConfig,
            tabletSnapshot->Settings.StoreWriterOptions,
            tabletSnapshot->PhysicalSchema,
            /*nameTable*/ nullptr,
            chunkWriter,
            /*dataSink*/ std::nullopt,
            chunkTimestamps,
            blockCache);
        auto hunkStoreRefs = orderedDynamicStore->GetHunkStoreRefs();
        tableWriter->GetMeta()->RegisterFinalizer(
            [
                weakUnderlying = MakeWeak(chunkWriter),
                hunkStoreRefs = hunkStoreRefs
            ] (TDeferredChunkMeta* meta) mutable {
                if (hunkStoreRefs.empty()) {
                    return;
                }

                auto underlying = weakUnderlying.Lock();
                YT_VERIFY(underlying);

                NTableClient::NProto::THunkChunkRefsExt hunkChunkRefsExt;
                ToProto(hunkChunkRefsExt.mutable_refs(), hunkStoreRefs);
                SetProtoExtension(meta->mutable_extensions(), hunkChunkRefsExt);
            });

        std::vector<TUnversionedRow> rows;
        rows.reserve(MaxRowsPerFlushRead);

        i64 rowCount = 0;

        YT_LOG_DEBUG("Ordered store flush started (StoreId: %v)",
            store->GetId());

        while (auto batch = reader->Read()) {
            auto rows = batch->MaterializeRows();
            if (rows.Empty()) {
                // NB: Memory store reader is always synchronous.
                YT_VERIFY(reader->GetReadyEvent().IsSet());
                continue;
            }

            rowCount += rows.size();
            if (!tableWriter->Write(rows)) {
                WaitFor(tableWriter->GetReadyEvent())
                    .ThrowOnError();
            }
        }

        if (rowCount == 0) {
            YT_LOG_DEBUG("Ordered store is empty, nothing to flush (StoreId: %v)",
                store->GetId());
            return TStoreFlushResult();
        }

        WaitFor(tableWriter->Close())
            .ThrowOnError();

        std::vector<TChunkInfo> chunkInfos{
            TChunkInfo{
                .ChunkId = tableWriter->GetChunkId(),
                .ChunkMeta = tableWriter->GetMeta(),
                .TabletId = tabletSnapshot->TabletId,
                .MountRevision = tabletSnapshot->MountRevision
            }
        };

        WaitFor(blockCache->Finish(chunkInfos))
            .ThrowOnError();

        auto dataStatistics = tableWriter->GetDataStatistics();
        auto diskSpace = CalculateDiskSpaceUsage(
            tabletSnapshot->Settings.StoreWriterOptions->ReplicationFactor,
            dataStatistics.regular_disk_space(),
            dataStatistics.erasure_disk_space());
        auto mediumThrottler = GetBlobMediumWriteThrottler(
            TabletContext_->GetDynamicConfigManager(),
            tabletSnapshot);

        YT_LOG_DEBUG("Throttling blobs media write in ordered store flush (DiskSpace: %v)",
            diskSpace);

        WaitFor(mediumThrottler->Throttle(diskSpace))
            .ThrowOnError();

        YT_LOG_DEBUG("Ordered store flushed (StoreId: %v, ChunkId: %v, DiskSpace: %v, HunkChunkIds: %v)",
            store->GetId(),
            chunkWriter->GetChunkId(),
            diskSpace,
            MakeFormattableView(hunkStoreRefs, [] (auto* builder, const auto& hunkStoreRef) {
                FormatValue(builder, hunkStoreRef.ChunkId, TStringBuf());
            }));

        TStoreFlushResult result;
        {
            auto& descriptor = result.StoresToAdd.emplace_back();
            descriptor.set_store_type(ToProto<int>(EStoreType::OrderedChunk));
            ToProto(descriptor.mutable_store_id(), chunkWriter->GetChunkId());
            *descriptor.mutable_chunk_meta() = *tableWriter->GetMeta();
            FilterProtoExtensions(descriptor.mutable_chunk_meta()->mutable_extensions(), GetMasterChunkMetaExtensionTagsFilter());
            descriptor.set_starting_row_index(orderedDynamicStore->GetStartingRowIndex());
        }

        for (const auto& hunkStoreRef : hunkStoreRefs) {
            auto& descriptor = result.HunkChunksToAdd.emplace_back();
            ToProto(descriptor.mutable_chunk_id(), hunkStoreRef.ChunkId);
            // TODO(aleksandra-zh): add meta as well.
        }

        return result;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

