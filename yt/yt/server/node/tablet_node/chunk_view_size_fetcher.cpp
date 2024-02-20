#include "chunk_view_size_fetcher.h"
#include "sorted_chunk_store.h"
#include "tablet.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_replica_cache.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_slice.h>
#include <yt/yt/ytlib/table_client/chunk_slice_size_fetcher.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NClusterNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkViewSizeFetcher
    : public TCompactionHintFetcher
{
public:
    TChunkViewSizeFetcher(
        TTabletCellId cellId,
        TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        IInvokerPtr heavyInvoker,
        NNative::IClientPtr client,
        IChunkReplicaCachePtr chunkReplicaCache,
        const TClusterNodeDynamicConfigPtr& config)
        : TCompactionHintFetcher(
            std::move(invoker),
            TabletNodeProfiler
                .WithPrefix("/chunk_view_size_fetcher")
                .WithTag("cell_id", ToString(cellId)))
        , NodeDirectory_(std::move(nodeDirectory))
        , HeavyInvoker_(std::move(heavyInvoker))
        , Client_(std::move(client))
        , ChunkReplicaCache_(std::move(chunkReplicaCache))
    {
        Reconfigure(config);
    }

    void Reconfigure(const TClusterNodeDynamicConfigPtr& config) override
    {
        const auto& storeCompactorConfig = config->TabletNode->StoreCompactor;
        FetchingExecutor_->SetPeriod(storeCompactorConfig->ChunkViewSizeFetchPeriod);
        RequestThrottler_->Reconfigure(storeCompactorConfig->ChunkViewSizeRequestThrottler);
    }

private:
    const TNodeDirectoryPtr NodeDirectory_;
    const IInvokerPtr HeavyInvoker_;
    const NNative::IClientPtr Client_;
    const IChunkReplicaCachePtr ChunkReplicaCache_;

    void ResetResult(const IStorePtr& store) const override
    {
        store->AsSortedChunk()->CompactionHints().ChunkViewSize.CompactionHint.reset();
    }

    bool IsFetchableStore(const IStorePtr& store) const override
    {
        return store->IsChunk() && TypeFromId(store->GetId()) == EObjectType::ChunkView;
    }

    bool IsFetchableTablet(const TTablet& tablet) const override
    {
        return tablet.GetSettings().MountConfig->EnableNarrowChunkViewCompaction && tablet.IsPhysicallySorted();
    }

    TCompactionHintFetchStatus& GetFetchStatus(const IStorePtr& store) const override
    {
        return store->AsSortedChunk()->CompactionHints().ChunkViewSize.FetchStatus;
    }

    void MakeRequest(std::vector<IStorePtr>&& stores) override
    {
        static constexpr int RequestStep = 1;

        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(!stores.empty());

        std::vector<TChunkId> chunkIds;
        for (const auto& store : stores) {
            GetFetchStatus(store).RequestStep = RequestStep;
            chunkIds.push_back(store->AsSortedChunk()->GetChunkId());
        }

        AllSet(ChunkReplicaCache_->GetReplicas(chunkIds))
            .SubscribeUnique(BIND(
                &TChunkViewSizeFetcher::OnChunkReplicasReceived,
                MakeStrong(this),
                std::move(stores))
                .Via(Invoker_));
    }

    void OnChunkReplicasReceived(
        const std::vector<IStorePtr>& stores,
        TErrorOr<std::vector<TErrorOr<NChunkClient::TAllyReplicasInfo>>> allSetResult)
    {
        static constexpr int RequestStep = 2;

        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(allSetResult.IsOK());
        const auto& errorOrRsps = allSetResult.Value();
        YT_VERIFY(stores.size() == errorOrRsps.size());

        std::vector<TInputChunkPtr> inputChunks;
        std::vector<TSortedChunkStorePtr> locatedStores;
        for (const auto& [store, errorOrRsp] : Zip(stores, errorOrRsps)) {
            auto sortedChunkStore = store->AsSortedChunk();

            if (!errorOrRsp.IsOK()) {
                OnRequestFailed(sortedChunkStore, RequestStep - 1);

                YT_LOG_WARNING(errorOrRsp, "Failed to locate chunk under chunk view "
                    "(StoreId: %v, ChunkId: %v)",
                    sortedChunkStore->GetId(),
                    sortedChunkStore->GetChunkId());
                continue;
            }

            auto& fetchingStatus = GetFetchStatus(store);
            if (IsValidStoreState(store) && fetchingStatus.ShouldConsumeRequestResult(RequestStep)) {
                inputChunks.push_back(ChunkViewToInputChunk(sortedChunkStore, errorOrRsp.Value()));
                locatedStores.push_back(std::move(sortedChunkStore));
                fetchingStatus.RequestStep = RequestStep;
            }
        }

        if (inputChunks.empty()) {
            YT_LOG_DEBUG("Failed to locate requested chunks");
            return;
        }

        auto config = New<TFetcherConfig>();
        auto sizeFetcher = New<TChunkSliceSizeFetcher>(
            config,
            NodeDirectory_,
            HeavyInvoker_,
            /*chunkScraper*/ nullptr,
            Client_,
            Logger);

        for (auto&& chunk : inputChunks) {
            sizeFetcher->AddChunk(std::move(chunk));
        }

        sizeFetcher->Fetch()
            .Subscribe(BIND(
                &TChunkViewSizeFetcher::OnChunkViewSizesReceived,
                MakeStrong(this),
                sizeFetcher,
                std::move(locatedStores))
                .Via(Invoker_));
    }

    void OnChunkViewSizesReceived(
        const TChunkSliceSizeFetcherPtr& sizeFetcher,
        const std::vector<TSortedChunkStorePtr>& stores,
        TError error)
    {
        static constexpr int RequestStep = 3;

        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (!error.IsOK()) {
            for (const auto& store : stores) {
                OnRequestFailed(store, RequestStep - 1);
            }
            YT_LOG_WARNING(error, "Failed to fetch chunk view sizes");
        } else {
            i64 finishedRequestCount = 0;
            for (const auto& [store, weightedChunk] : Zip(stores, sizeFetcher->WeightedChunks())) {
                YT_VERIFY(weightedChunk->GetInputChunk()->GetChunkId() == store->GetChunkId());

                if (IsValidStoreState(store) && GetFetchStatus(store).ShouldConsumeRequestResult(RequestStep)) {
                    auto miscExt = GetProtoExtension<TMiscExt>(store->GetChunkMeta().extensions());
                    i64 chunkDataWeight = miscExt.data_weight();
                    i64 chunkViewDataWeight = weightedChunk->GetDataWeight();

                    double share = static_cast<double>(chunkViewDataWeight) / chunkDataWeight;
                    double maxChunkViewSizeRatio = store->GetTablet()->GetSettings().MountConfig->MaxChunkViewSizeRatio;

                    auto& compactionHint = store->AsSortedChunk()->CompactionHints().ChunkViewSize;
                    compactionHint.FetchStatus.RequestStep = RequestStep;
                    compactionHint.CompactionHint = share <= maxChunkViewSizeRatio
                        ? EChunkViewSizeStatus::CompactionRequired
                        : EChunkViewSizeStatus::CompactionNotRequired;

                    ++finishedRequestCount;
                    YT_LOG_DEBUG("Finished fetching chunk view size (StoreId: %v, ChunkId: %v, CompactionHint: %v)",
                        store->GetId(),
                        store->GetChunkId(),
                        compactionHint.CompactionHint);
                }
            }

            FinishedRequestCount_.Increment(finishedRequestCount);
        }
    }

    TInputChunkPtr ChunkViewToInputChunk(
        const TSortedChunkStorePtr& store,
        const NChunkClient::TAllyReplicasInfo& replicas) const
    {
        NChunkClient::NProto::TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), store->GetChunkId());
        ToProto(chunkSpec.mutable_replicas(), replicas.Replicas);
        ToProto(chunkSpec.mutable_legacy_replicas(), TChunkReplicaWithMedium::ToChunkReplicas(replicas.Replicas));
        *chunkSpec.mutable_chunk_meta() = store->GetChunkMeta();

        if (auto lowerLimit = store->GetChunkViewLowerLimit()) {
            ToProto(chunkSpec.mutable_lower_limit(), *lowerLimit);
        }
        if (auto upperLimit = store->GetChunkViewUpperLimit()) {
            ToProto(chunkSpec.mutable_upper_limit(), *upperLimit);
        }

        chunkSpec.set_erasure_codec(ToProto<int>(store->GetErasureCodecId()));

        return New<TInputChunk>(chunkSpec);
    }
};

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetcherPtr CreateChunkViewSizeFetcher(
    TTabletCellId cellId,
    TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    IInvokerPtr heavyInvoker,
    NNative::IClientPtr client,
    IChunkReplicaCachePtr chunkReplicaCache,
    const TClusterNodeDynamicConfigPtr& config)
{
    return New<TChunkViewSizeFetcher>(
        cellId,
        std::move(nodeDirectory),
        std::move(invoker),
        std::move(heavyInvoker),
        std::move(client),
        std::move(chunkReplicaCache),
        config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
