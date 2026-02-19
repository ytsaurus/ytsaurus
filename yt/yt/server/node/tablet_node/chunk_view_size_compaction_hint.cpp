#include "chunk_view_size_compaction_hint.h"
#include "compaction_hint_fetching.h"
#include "tablet.h"
#include "sorted_chunk_store.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_replica_cache.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/table_client/chunk_slice.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_slice_size_fetcher.h>

namespace NYT::NTabletNode {

using namespace NProfiling;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NObjectClient;
using namespace NChunkClient::NProto;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TChunkViewSizeFetchPipeline
    : public TCompactionHintFetchPipeline
{
public:
    using TCompactionHintFetchPipeline::TCompactionHintFetchPipeline;

protected:
    virtual NLsm::EStoreCompactionHintKind GetStoreCompactionHintKind() const override
    {
        return NLsm::EStoreCompactionHintKind::ChunkViewTooNarrow;
    }

private:
    void DoFetch() override
    {
        YT_VERIFY(TypeFromId(Store_->GetId()) == EObjectType::ChunkView);

        SubscribeWithErrorHandling(
            Store_->GetTablet()->GetChunkReplicaCache()->GetReplicas({Store_->GetChunkId()})[0],
            std::bind_front(&TChunkViewSizeFetchPipeline::OnChunkReplicaReceived, this));
    }

    void OnChunkReplicaReceived(const NChunkClient::TAllyReplicasInfo& replicas)
    {
        const auto& Logger = GetFetcher()->Context().Logger;

        auto* tablet = Store_->GetTablet();

        const auto& client = tablet->GetClient();

        auto sizeFetcher = New<TChunkSliceSizeFetcher>(
            New<TFetcherConfig>(),
            client->GetNativeConnection()->GetNodeDirectory(),
            tablet->GetStorageHeavyInvoker(),
            /*chunkScraper*/ nullptr,
            client,
            Logger());

        sizeFetcher->AddChunk(ChunkViewToInputChunk(replicas));

        SubscribeWithErrorHandling(
            sizeFetcher->Fetch(),
            std::bind_front(&TChunkViewSizeFetchPipeline::OnChunkViewSizeReceived, this, sizeFetcher));
    }

    void OnChunkViewSizeReceived(const TChunkSliceSizeFetcherPtr& sizeFetcher)
    {
        const auto& weightedChunk = sizeFetcher->WeightedChunks()[0];
        YT_VERIFY(weightedChunk->GetInputChunk()->GetChunkId() == Store_->GetChunkId());

        auto miscExt = GetProtoExtension<TMiscExt>(Store_->GetChunkMeta().extensions());
        i64 chunkDataWeight = miscExt.data_weight();
        i64 chunkViewDataWeight = weightedChunk->GetDataWeight();

        double share = static_cast<double>(chunkViewDataWeight) / chunkDataWeight;

        FinishFetch(share);
    }

    TInputChunkPtr ChunkViewToInputChunk(const NChunkClient::TAllyReplicasInfo& replicas) const
    {
        NChunkClient::NProto::TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), Store_->GetChunkId());
        ToProto(chunkSpec.mutable_replicas(), replicas.Replicas);
        *chunkSpec.mutable_chunk_meta() = Store_->GetChunkMeta();

        if (auto lowerLimit = Store_->GetChunkViewLowerLimit()) {
            ToProto(chunkSpec.mutable_lower_limit(), *lowerLimit);
        }
        if (auto upperLimit = Store_->GetChunkViewUpperLimit()) {
            ToProto(chunkSpec.mutable_upper_limit(), *upperLimit);
        }

        chunkSpec.set_erasure_codec(ToProto(Store_->GetErasureCodecId()));

        return New<TInputChunk>(chunkSpec);
    }
};

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetchPipelinePtr CreateChunkViewSizeFetchPipeline(TSortedChunkStore* store)
{
    return New<TChunkViewSizeFetchPipeline>(store);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
