#include "min_hash_digest_fetcher.h"

#include "config.h"
#include "sorted_chunk_store.h"
#include "tablet.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/yt/library/min_hash_digest/min_hash_digest.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TMinHashDigestFetcher
    : public TCompactionHintFetcher
{
public:
    TMinHashDigestFetcher(
        TTabletCellId cellId,
        IInvokerPtr invoker,
        INodeMemoryTrackerPtr memoryTracker,
        const NClusterNode::TClusterNodeDynamicConfigPtr& config)
        : TCompactionHintFetcher(
            std::move(invoker),
            TabletNodeProfiler()
                .WithPrefix("/min_hash_digest")
                .WithTag("cell_id", ToString(cellId)))
        , MemoryTracker_(std::move(memoryTracker))
    {
        Reconfigure(config);
    }

    void Reconfigure(const TClusterNodeDynamicConfigPtr& config) override
    {
        auto fetchConfig = config->TabletNode->StoreCompactor->MinHashDigestFetcher;
        FetchingExecutor_->SetPeriod(fetchConfig->FetchPeriod);
        RequestThrottler_->Reconfigure(fetchConfig->RequestThrottler);
    }

    void ReconfigureTablet(TTablet* tablet, const TTableSettings& oldSettings) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        bool oldEnable = oldSettings.MountConfig->MinHashDigestCompaction->Enable;
        bool newEnable = tablet->GetSettings().MountConfig->MinHashDigestCompaction->Enable;

        if (newEnable && !oldEnable) {
            FetchStoreInfos(tablet);
        }
        if (!newEnable && oldEnable) {
            ResetCompactionHints(tablet);
        }
    }

private:
    const INodeMemoryTrackerPtr MemoryTracker_;

    static void LogNoMinHashDigest(const TSortedChunkStorePtr& sortedChunkStore)
    {
        YT_LOG_DEBUG("Chunk does not contain min hash row digest (StoreId: %v, ChunkId: %v)",
            sortedChunkStore->GetId(),
            sortedChunkStore->GetChunkId());
    }

    TClientChunkReadOptions CreateChunkReadOptions()
    {
        return {
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletCompaction),
            .ReadSessionId = TReadSessionId::Create(),
            .MemoryUsageTracker = MemoryTracker_->WithCategory(EMemoryCategory::TabletBackground),
        };
    }

    void ResetResult(const IStorePtr& store) const override
    {
        store->AsSortedChunk()->CompactionHints().MinHashDigest.CompactionHint.reset();
    }

    bool IsFetchableStore(const IStorePtr& store) const override
    {
        return store->GetType() == EStoreType::SortedChunk;
    }

    bool IsFetchableTablet(const TTablet& tablet) const override
    {
        return tablet.GetSettings().MountConfig->MinHashDigestCompaction->Enable &&
            tablet.IsPhysicallySorted();
    }

    TCompactionHintFetchStatus& GetFetchStatus(const IStorePtr& store) const override
    {
        return store->AsSortedChunk()->CompactionHints().MinHashDigest.FetchStatus;
    }

    void MakeRequest(std::vector<IStorePtr>&& stores) override
    {
        static constexpr int RequestStep = 1;

        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(!stores.empty());

        std::vector<IStorePtr> storesToNextStep;

        std::vector<IStorePtr> storesToFetch;
        std::vector<TFuture<TCachedVersionedChunkMetaPtr>> asyncChunkMetas;

        for (const auto& store : stores) {
            auto sortedChunkStore = store->AsSortedChunk();
            auto minHashDigestBlockIndex = sortedChunkStore->GetMinHashDigestBlockIndex();

            if (minHashDigestBlockIndex.IsFetched()) {
                if (minHashDigestBlockIndex.IsFound()) {
                    GetFetchStatus(store).RequestStep = RequestStep + 1;
                    storesToNextStep.push_back(std::move(sortedChunkStore));
                } else {
                    LogNoMinHashDigest(sortedChunkStore);
                }

                continue;
            }

            YT_LOG_DEBUG("Requesting chunk meta for store in min hash row digest fetcher (StoreId: %v, ChunkId: %v)",
                sortedChunkStore->GetId(),
                sortedChunkStore->GetChunkId());

            GetFetchStatus(store).RequestStep = RequestStep;
            storesToFetch.push_back(sortedChunkStore);

            const auto& reader = sortedChunkStore->GetBackendReaders(
                EWorkloadCategory::SystemTabletCompaction).ChunkReader;

            asyncChunkMetas.push_back(sortedChunkStore->GetCachedVersionedChunkMeta(
                reader,
                CreateChunkReadOptions(),
                /*prepareColumnMeta*/ false));
        }

        ProcessStoresWithFilledMeta(storesToNextStep);

        if (!storesToFetch.empty()) {
            AllSet(std::move(asyncChunkMetas))
                .AsUnique().Subscribe(BIND(
                    &TMinHashDigestFetcher::OnChunkMetaReceived,
                    MakeStrong(this),
                    Passed(std::move(storesToFetch)))
                    .Via(Invoker_));
        }
    }

    TFuture<std::vector<TBlock>> ProcessStoreWithFilledMeta(const IStorePtr& store)
    {
        static constexpr int RequestStep = 2;

        auto sortedChunkStore = store->AsSortedChunk();
        auto& minHashDigestCompactionHint = sortedChunkStore->CompactionHints().MinHashDigest.CompactionHint;

        int blockIndex = sortedChunkStore->GetMinHashDigestBlockIndex().GetBlockIndex();
        const auto& blockCache = sortedChunkStore->GetBlockCache();

        auto blockData = blockCache->FindBlock(
            TBlockId(sortedChunkStore->GetChunkId(), blockIndex),
            EBlockType::MinHashDigest).Data;

        if (blockData) {
            GetFetchStatus(store).RequestStep = RequestStep + 1;
            minHashDigestCompactionHint = BuildMinHashDigest(std::move(blockData));

            return {std::nullopt};
        } else {
            auto reader = sortedChunkStore->GetBackendReaders(
                EWorkloadCategory::SystemTabletCompaction).ChunkReader;

            return reader->ReadBlocks(
                IChunkReader::TReadBlocksOptions{
                    .ClientOptions = CreateChunkReadOptions(),
                },
                {blockIndex});
        }
    }

    void ProcessStoresWithFilledMeta(const std::vector<IStorePtr>& stores)
    {
        std::vector<IStorePtr> storesToFetch;
        std::vector<TFuture<std::vector<TBlock>>> asyncMinHashDigests;

        i64 finishedRequestCount = 0;
        for (const auto& store : stores) {
            if (auto asyncMinHashDigest = ProcessStoreWithFilledMeta(store)) {
                storesToFetch.push_back(store);
                asyncMinHashDigests.push_back(std::move(asyncMinHashDigest));
            } else {
                // NB: We already have block and successfully write digest to store.
                ++finishedRequestCount;
            }
        }

        FinishedRequestCount_.Increment(finishedRequestCount);

        if (!storesToFetch.empty()) {
            AllSet(std::move(asyncMinHashDigests))
                .AsUnique().Subscribe(BIND(
                    &TMinHashDigestFetcher::OnMinHashDigestReceived,
                    MakeStrong(this),
                    std::move(storesToFetch))
                    .Via(Invoker_));
        }
    }

    void OnChunkMetaReceived(
        std::vector<IStorePtr> stores,
        TErrorOr<std::vector<TErrorOr<TCachedVersionedChunkMetaPtr>>> allSetResult)
    {
        static constexpr int RequestStep = 2;

        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(allSetResult.IsOK());
        auto errorOrRsps = allSetResult.Value();
        YT_VERIFY(stores.size() == errorOrRsps.size());

        std::vector<IStorePtr> storesReadyToFetch;

        for (const auto& [store, errorOrRsp] : Zip(stores, errorOrRsps)) {
            if (!ShouldConsumeRequestResultForStore(store, RequestStep)) {
                continue;
            }

            // TODO(dave11ar): Maybe skip if block index is set?
            if (!errorOrRsp.IsOK()) {
                OnRequestFailed(store, RequestStep - 1);
                YT_LOG_WARNING(errorOrRsp, "Failed to receive chunk meta for store in min hash row digest fetcher (StoreId: %v)",
                    store->GetId());
                continue;
            }

            GetFetchStatus(store).RequestStep = RequestStep;

            auto sortedChunkStore = store->AsSortedChunk();
            auto minHashDigestBlockIndex = sortedChunkStore->GetMinHashDigestBlockIndex();
            YT_VERIFY(minHashDigestBlockIndex.IsFetched());

            if (!minHashDigestBlockIndex.IsFound()) {
                LogNoMinHashDigest(sortedChunkStore);
                continue;
            }

            storesReadyToFetch.push_back(std::move(sortedChunkStore));
        }

        ProcessStoresWithFilledMeta(storesReadyToFetch);
    }

    void OnMinHashDigestReceived(
        const std::vector<IStorePtr>& stores,
        TErrorOr<std::vector<TErrorOr<std::vector<TBlock>>>> allSetResult)
    {
        static constexpr int RequestStep = 3;

        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(allSetResult.IsOK());
        auto errorOrRsps = allSetResult.Value();
        YT_VERIFY(stores.size() == errorOrRsps.size());

        i64 finishedRequestCount = 0;
        for (const auto& [store, errorOrRsp] : Zip(stores, errorOrRsps)) {
            if (!ShouldConsumeRequestResultForStore(store, RequestStep)) {
                continue;
            }

            if (!errorOrRsp.IsOK()) {
                OnRequestFailed(store, RequestStep - 1);
                YT_LOG_WARNING(errorOrRsp, "Failed to receive min hash row digest (StoreId: %v)",
                    store->GetId());
                continue;
            }

            auto sortedChunkStore = store->AsSortedChunk();
            auto& minHashDigestCompactionHint = sortedChunkStore->CompactionHints().MinHashDigest.CompactionHint;

            GetFetchStatus(store).RequestStep = RequestStep;

            const auto& block = errorOrRsp.Value()[0];

            auto* codec = NCompression::GetCodec(sortedChunkStore->GetCompressionCodecId());
            auto decompressedBlockData = codec->Decompress(std::move(block.Data));

            sortedChunkStore->GetBlockCache()->PutBlock(
                NChunkClient::TBlockId(sortedChunkStore->GetChunkId(), sortedChunkStore->GetMinHashDigestBlockIndex().GetBlockIndex()),
                EBlockType::MinHashDigest,
                TBlock(decompressedBlockData));

            minHashDigestCompactionHint = BuildMinHashDigest(std::move(decompressedBlockData));
        }

        FinishedRequestCount_.Increment(finishedRequestCount);
    }

    TMinHashDigestPtr BuildMinHashDigest(TSharedRef data)
    {
        auto MinHashDigest = New<TMinHashDigest>();
        MinHashDigest->Initialize(data);
        return MinHashDigest;
    }
};

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetcherPtr CreateMinHashDigestFetcher(
    TTabletCellId cellId,
    IInvokerPtr invoker,
    INodeMemoryTrackerPtr memoryTracker,
    const NClusterNode::TClusterNodeDynamicConfigPtr& config)
{
    return New<TMinHashDigestFetcher>(
        cellId,
        std::move(invoker),
        std::move(memoryTracker),
        config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
