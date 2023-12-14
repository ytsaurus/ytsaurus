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
        TClusterNodeDynamicConfigPtr config,
        TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        IInvokerPtr heavyInvoker,
        NNative::IClientPtr client,
        IChunkReplicaCachePtr chunkReplicaCache)
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
        Invoker_->Invoke(BIND(
            &TCompactionHintFetcher::Reconfigure,
            MakeWeak(this),
            /*oldConfig*/ nullptr,
            std::move(config)));
    }

    void Reconfigure(
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newConfig) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& config = newConfig->TabletNode->StoreCompactor;
        RequestThrottler_->Reconfigure(config->ChunkViewSizeRequestThrottler);
        ThrottlerTryAcquireBackoff_ = config->ChunkViewSizeThrottlerTryAcquireBackoff;
    }

private:
    const TNodeDirectoryPtr NodeDirectory_;
    const IInvokerPtr HeavyInvoker_;
    const NNative::IClientPtr Client_;
    const IChunkReplicaCachePtr ChunkReplicaCache_;

    static TCompactionHintRequestWithResult<EChunkViewSizeStatus>& GetChunkViewCompactionHints(
        const IStorePtr& store)
    {
        return store->AsSortedChunk()->CompactionHints().ChunkViewSize;
    }

    bool HasRequestStatus(const IStorePtr& store) const override
    {
        return GetChunkViewCompactionHints(store).IsRequestStatus();
    }

    ECompactionHintRequestStatus GetRequestStatus(const IStorePtr& store) const override
    {
        return GetChunkViewCompactionHints(store).AsRequestStatus();
    }

    void SetRequestStatus(const IStorePtr& store, ECompactionHintRequestStatus status) const override
    {
        GetChunkViewCompactionHints(store).SetRequestStatus(status);
    }

    bool IsFetchingRequiredForStore(const IStorePtr& store) const override
    {
        return store->IsChunk() && TypeFromId(store->GetId()) == EObjectType::ChunkView;
    }

    bool IsFetchingRequiredForTablet(const TTablet& tablet) const override
    {
        return tablet.GetSettings().MountConfig->EnableNarrowChunkViewCompaction && tablet.IsPhysicallySorted();
    }

    void TryMakeRequest() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto stores = GetStoresForRequest();
        if (stores.empty()) {
            return;
        }

        std::vector<TChunkId> chunkIds;
        for (const auto& store : stores) {
            SetRequestStatus(store, ECompactionHintRequestStatus::Requested);
            chunkIds.push_back(store->AsSortedChunk()->GetChunkId());
        }

        auto replicasFutures = ChunkReplicaCache_->GetReplicas(chunkIds);
        AllSet(std::move(replicasFutures))
            .SubscribeUnique(BIND(
                &TChunkViewSizeFetcher::OnChunkReplicasReceived,
                MakeStrong(this),
                std::move(stores))
                .Via(Invoker_));
    }

    void OnChunkReplicasReceived(
        std::vector<IStorePtr> stores,
        TErrorOr<std::vector<TErrorOr<NChunkClient::TAllyReplicasInfo>>> allSetResult)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(allSetResult.IsOK());
        const auto& errorOrRsps = allSetResult.Value();

        YT_VERIFY(stores.size() == errorOrRsps.size());

        std::vector<TInputChunkPtr> inputChunks;
        std::vector<TSortedChunkStorePtr> locatedStores;
        for (const auto& [store, errorOrRsp] : Zip(stores, errorOrRsps)) {
            auto sortedChunkStore = store->AsSortedChunk();

            if (!errorOrRsp.IsOK()) {
                OnRequestFailed(sortedChunkStore);

                YT_LOG_WARNING(errorOrRsp, "Failed to locate chunk under chunk view "
                    "(StoreId: %v, ChunkId: %v)",
                    sortedChunkStore->GetId(),
                    sortedChunkStore->GetChunkId());
                continue;
            }

            inputChunks.push_back(ChunkViewToInputChunk(sortedChunkStore, errorOrRsp.Value()));
            locatedStores.push_back(std::move(sortedChunkStore));
        }

        if (inputChunks.empty()) {
            YT_LOG_DEBUG("Failed to locate requested chunks");
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
        std::vector<TSortedChunkStorePtr> stores,
        TError error)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (!error.IsOK()) {
            YT_LOG_WARNING(error, "Failed to fetch chunk view sizes");
            for (const auto& store : stores) {
                OnRequestFailed(store);
            }
        } else {
            for (const auto& [store, weightedChunk] : Zip(stores, sizeFetcher->WeightedChunks())) {
                YT_VERIFY(weightedChunk->GetInputChunk()->GetChunkId() == store->GetChunkId());

                auto miscExt = GetProtoExtension<TMiscExt>(store->GetChunkMeta().extensions());
                i64 chunkDataWeight = miscExt.data_weight();
                auto chunkViewDataWeight = weightedChunk->GetDataWeight();

                double share = static_cast<double>(chunkViewDataWeight) / chunkDataWeight;
                double maxChunkViewSizeRatio = store->GetTablet()->GetSettings().MountConfig->MaxChunkViewSizeRatio;
                GetChunkViewCompactionHints(store).SetResult(share <= maxChunkViewSizeRatio
                    ? EChunkViewSizeStatus::CompactionRequired
                    : EChunkViewSizeStatus::CompactionNotRequired);
            }
        }

        TryMakeRequest();
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
    TClusterNodeDynamicConfigPtr config,
    TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    IInvokerPtr heavyInvoker,
    NNative::IClientPtr client,
    IChunkReplicaCachePtr chunkReplicaCache)
{
    return New<TChunkViewSizeFetcher>(
        cellId,
        std::move(config),
        std::move(nodeDirectory),
        std::move(invoker),
        std::move(heavyInvoker),
        std::move(client),
        std::move(chunkReplicaCache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
