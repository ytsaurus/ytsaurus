#include "chunk_view_size_fetcher.h"
#include "sorted_chunk_store.h"
#include "tablet.h"

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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkViewSizeFetcher
    : public IChunkViewSizeFetcher
{
public:
    TChunkViewSizeFetcher(
        TTabletCellId cellId,
        TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr guardedAutomatonInvoker,
        IInvokerPtr heavyInvoker,
        NNative::IClientPtr client,
        IChunkReplicaCachePtr chunkReplicaCache)
        : Profiler_(TabletNodeProfiler
            .WithPrefix("/chunk_view_size_fetcher")
            .WithTag("cell_id", ToString(cellId)))
        , RequestedCounter_(Profiler_.Counter("/request_count"))
        , FailedCounter_(Profiler_.Counter("/failed_request_count"))
        , NodeDirectory_(std::move(nodeDirectory))
        , GuardedAutomatonInvoker_(std::move(guardedAutomatonInvoker))
        , HeavyInvoker_(std::move(heavyInvoker))
        , Client_(std::move(client))
        , ChunkReplicaCache_(std::move(chunkReplicaCache))
    { }

    void FetchChunkViewSizes(
        TTablet* tablet,
        std::optional<TRange<IStorePtr>> stores) override
    {
        if (!tablet->GetSettings().MountConfig->EnableNarrowChunkViewCompaction) {
            return;
        }

        if (!tablet->IsPhysicallySorted()) {
            return;
        }

        std::vector<TSortedChunkStorePtr> storesAwaitingChinkViewSize;
        auto onStore = [&] (auto store) {
            if (store->IsChunk() && TypeFromId(store->GetId()) == EObjectType::ChunkView) {
                YT_VERIFY(store->IsSorted());

                auto sortedChunkStore = store->AsSortedChunk();
                if (sortedChunkStore->GetChunkViewSizeFetchStatus() == EChunkViewSizeFetchStatus::None) {
                    storesAwaitingChinkViewSize.push_back(std::move(sortedChunkStore));
                }
            }
        };

        if (stores) {
            for (const auto& store : *stores) {
                onStore(store);
            }
        } else {
            for (const auto& [storeId, store] : tablet->StoreIdMap()) {
                onStore(store);
            }
        }

        DoFetchRequests(
            tablet->GetSettings().MountConfig->MaxChunkViewSizeRatio,
            storesAwaitingChinkViewSize);
    }

private:
    const TProfiler Profiler_;
    const NProfiling::TCounter RequestedCounter_;
    const NProfiling::TCounter FailedCounter_;

    const TNodeDirectoryPtr NodeDirectory_;
    const IInvokerPtr GuardedAutomatonInvoker_;
    const IInvokerPtr HeavyInvoker_;
    const NNative::IClientPtr Client_;
    const IChunkReplicaCachePtr ChunkReplicaCache_;

    struct TChunkViewMetaRequest
    {
        TSortedChunkStorePtr Store;
        TFuture<TRefCountedChunkMetaPtr> AsyncMeta;
    };

    void DoFetchRequests(
        double maxChunkViewSizeRatio,
        const std::vector<TSortedChunkStorePtr>& storesAwaitingChinkViewSize)
    {
        if (storesAwaitingChinkViewSize.empty()) {
            return;
        }

        RequestedCounter_.Increment(std::ssize(storesAwaitingChinkViewSize));

        std::vector<TChunkId> chunkIds;
        for (const auto& store : storesAwaitingChinkViewSize) {
            chunkIds.push_back(store->GetChunkId());
            store->SetChunkViewSizeFetchStatus(EChunkViewSizeFetchStatus::Requested);
        }

        auto replicasFutures = ChunkReplicaCache_->GetReplicas(chunkIds);
        AllSet(std::move(replicasFutures))
            .SubscribeUnique(BIND(
                &TChunkViewSizeFetcher::OnChunkReplicasReceived,
                MakeStrong(this),
                std::move(storesAwaitingChinkViewSize),
                maxChunkViewSizeRatio)
                .Via(GuardedAutomatonInvoker_));
    }

    void OnChunkReplicasReceived(
        std::vector<TSortedChunkStorePtr> stores,
        double maxChunkViewSizeRatio,
        TErrorOr<std::vector<TErrorOr<NChunkClient::TAllyReplicasInfo>>> allSetResult)
    {
        YT_VERIFY(allSetResult.IsOK());
        const auto& errorOrRsps = allSetResult.Value();

        YT_VERIFY(stores.size() == errorOrRsps.size());

        std::vector<TInputChunkPtr> inputChunks;
        std::vector<TSortedChunkStorePtr> locatedStores;
        for (const auto& [store, errorOrRsp] : Zip(stores, errorOrRsps)) {
            if (!errorOrRsp.IsOK()) {
                FailedCounter_.Increment();
                store->SetChunkViewSizeFetchStatus(EChunkViewSizeFetchStatus::None);

                YT_LOG_WARNING(errorOrRsp, "Failed to locate chunk under chunk view "
                    "(StoreId: %v, ChunkId: %v)",
                    store->GetId(),
                    store->GetChunkId());
                continue;
            }

            inputChunks.push_back(ChunkViewToInputChunk(store, errorOrRsp.Value()));
            locatedStores.push_back(std::move(store));
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
                std::move(locatedStores),
                maxChunkViewSizeRatio)
                .Via(GuardedAutomatonInvoker_));
    }

    void OnChunkViewSizesReceived(
        const TChunkSliceSizeFetcherPtr& sizeFetcher,
        std::vector<TSortedChunkStorePtr> stores,
        double maxChunkViewSizeRatio,
        TError error)
    {
        if (!error.IsOK()) {
            YT_LOG_WARNING(error, "Failed to fetch chunk view sizes");
            FailedCounter_.Increment(std::ssize(stores));

            for (const auto& store : stores) {
                store->SetChunkViewSizeFetchStatus(EChunkViewSizeFetchStatus::None);
            }

            return;
        }

        for (const auto& [store, weightedChunk] : Zip(stores, sizeFetcher->WeightedChunks())) {
            YT_VERIFY(weightedChunk->GetInputChunk()->GetChunkId() == store->GetChunkId());

            auto miscExt = GetProtoExtension<TMiscExt>(store->GetChunkMeta().extensions());
            i64 chunkDataWeight = miscExt.data_weight();
            auto chunkViewDataWeight = weightedChunk->GetDataWeight();

            double share = static_cast<double>(chunkViewDataWeight) / chunkDataWeight;
            store->SetChunkViewSizeFetchStatus(share <= maxChunkViewSizeRatio
                ? EChunkViewSizeFetchStatus::CompactionRequired
                : EChunkViewSizeFetchStatus::CompactionNotRequired);
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

DEFINE_REFCOUNTED_TYPE(TChunkViewSizeFetcher)

////////////////////////////////////////////////////////////////////////////////

IChunkViewSizeFetcherPtr CreateChunkViewSizeFetcher(
    TTabletCellId cellId,
    TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr guardedAutomatonInvoker,
    IInvokerPtr heavyInvoker,
    NNative::IClientPtr client,
    IChunkReplicaCachePtr chunkReplicaCache)
{
    return New<TChunkViewSizeFetcher>(
        cellId,
        std::move(nodeDirectory),
        std::move(guardedAutomatonInvoker),
        std::move(heavyInvoker),
        std::move(client),
        std::move(chunkReplicaCache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
