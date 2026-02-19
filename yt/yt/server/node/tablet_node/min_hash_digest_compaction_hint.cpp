#include "min_hash_digest_compaction_hint.h"
#include "compaction_hint_fetching.h"
#include "tablet.h"
#include "sorted_chunk_store.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/yt/core/misc/sync_cache.h>

#include <yt/yt/library/min_hash_digest/min_hash_digest.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TMinHashDigestFetchPipeline
    : public TCompactionHintFetchPipeline
{
public:
    using TCompactionHintFetchPipeline::TCompactionHintFetchPipeline;

protected:
    virtual NLsm::EStoreCompactionHintKind GetStoreCompactionHintKind() const override
    {
        return NLsm::EStoreCompactionHintKind::MinHashDigest;
    }

private:
    IMemoryUsageTrackerPtr MaybeGetMemoryUsageTracker() const
    {
        auto nodeMemoryTracker = Store_->GetTablet()->MaybeGetNodeMemoryUsageTracker();
        return nodeMemoryTracker
            ? nodeMemoryTracker->WithCategory(EMemoryCategory::TabletBackground)
            : nullptr;
    }

    TClientChunkReadOptions CreateChunkReadOptions() const
    {
        return {
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletCompaction),
            .ReadSessionId = TReadSessionId::Create(),
            .MemoryUsageTracker = MaybeGetMemoryUsageTracker(),
        };
    }

    virtual void DoFetch() override
    {
        if (auto* cacheElement = Store_->GetTablet()->GetMinHashDigestCache()->Find(Store_->GetChunkId())) {
            FinishFetch(*cacheElement);
            return;
        }

        auto blockIndex = Store_->GetMinHashDigestBlockIndex();

        auto chunkReader = Store_->GetBackendReaders(EWorkloadCategory::SystemTabletCompaction).ChunkReader;
        auto chunkReadOptions = CreateChunkReadOptions();

        if (!blockIndex.IsFetched()) {
            SubscribeWithErrorHandling(
                Store_->GetCachedVersionedChunkMeta(chunkReader, chunkReadOptions, /*prepareColumnMeta*/ false),
                std::bind_front(
                    &TMinHashDigestFetchPipeline::MakeMinHashDigestRequest,
                    this,
                    chunkReader,
                    chunkReadOptions));
            return;
        }

        MakeMinHashDigestRequest(chunkReader, chunkReadOptions);
    }

    void MakeMinHashDigestRequest(
        const IChunkReaderPtr& chunkReader,
        const TClientChunkReadOptions& chunkReadOptions)
    {
        auto blockIndex = Store_->GetMinHashDigestBlockIndex();
        YT_VERIFY(blockIndex.IsFetched());

        if (!blockIndex.IsFound()) {
            OnStoreHasNoHint();
            return;
        }

        SubscribeWithErrorHandling(
            chunkReader->ReadBlocks(
                IChunkReader::TReadBlocksOptions{
                    .ClientOptions = chunkReadOptions
                },
                {blockIndex.GetBlockIndex()}).AsUnique(),
            std::bind_front(&TMinHashDigestFetchPipeline::OnMinHashDigestReceived, this));
    }

    void OnMinHashDigestReceived(std::vector<TBlock>&& blocks)
    {
        const auto& Logger = GetFetcher()->Context().Logger;

        YT_VERIFY(blocks.size() == 1);

        TSharedRef decompressedBlockData;
        auto minHashDigest = New<TMinHashDigest>(MaybeGetMemoryUsageTracker());

        ExecuteParse([&] () {
            decompressedBlockData = NCompression::GetCodec(Store_->GetCompressionCodecId())->Decompress(std::move(blocks[0].Data));
            minHashDigest->Initialize(decompressedBlockData);
        });

        YT_LOG_DEBUG("Put min hash digest block in block cache (StoreId: %v, ChunkId: %v)",
            Store_->GetId(),
            Store_->GetChunkId());
        Store_->GetBlockCache()->PutBlock(
            NChunkClient::TBlockId(Store_->GetChunkId(), Store_->GetMinHashDigestBlockIndex().GetBlockIndex()),
            EBlockType::MinHashDigest,
            TBlock(decompressedBlockData));

        YT_LOG_DEBUG("Put min hash digest in deserialized cache (StoreId: %v, ChunkId: %v)",
            Store_->GetId(),
            Store_->GetChunkId());
        Store_->GetTablet()->GetMinHashDigestCache()->Insert(Store_->GetChunkId(), minHashDigest);

        FinishFetch(std::move(minHashDigest));
    }
};

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetchPipelinePtr CreateMinHashDigestFetchPipeline(TSortedChunkStore* store)
{
    return New<TMinHashDigestFetchPipeline>(store);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
