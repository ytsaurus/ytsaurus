#include "chunk_block_manager.h"

#include "private.h"
#include "blob_reader_cache.h"
#include "chunk.h"
#include "chunk_registry.h"
#include "config.h"
#include "location.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/file_reader.h>

#include <yt/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/ytalloc/memory_zone.h>

namespace NYT::NDataNode {

using namespace NObjectClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NClusterNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TChunkBlockManager
    : public IChunkBlockManager
{
public:
    explicit TChunkBlockManager(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual TFuture<std::vector<TBlock>> ReadBlockRange(
        TChunkId chunkId,
        int firstBlockIndex,
        int blockCount,
        const TBlockReadOptions& options) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        try {
            const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
            // NB: At the moment, range read requests are only possible for the whole chunks.
            auto chunk = chunkRegistry->GetChunkOrThrow(chunkId);
            return chunk->ReadBlockRange(
                firstBlockIndex,
                blockCount,
                options);
        } catch (const std::exception& ex) {
            return MakeFuture<std::vector<TBlock>>(TError(ex));
        }
    }

    virtual TFuture<std::vector<TBlock>> ReadBlockSet(
        TChunkId chunkId,
        const std::vector<int>& blockIndexes,
        const TBlockReadOptions& options) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        try {
            const auto& chunkRegistry = Bootstrap_->GetChunkRegistry();
            auto chunk = chunkRegistry->FindChunk(chunkId);
            auto type = TypeFromId(DecodeChunkId(chunkId).Id);
            if (!chunk) {
                std::vector<TBlock> blocks;
                // During block peering, data nodes exchange individual blocks.
                // Thus the cache may contain a block not bound to any chunk in the registry.
                // We must look for these blocks.
                if (options.BlockCache &&
                    options.FetchFromCache &&
                    (type == EObjectType::Chunk || type == EObjectType::ErasureChunk))
                {
                    for (int blockIndex : blockIndexes) {
                        auto blockId = TBlockId(chunkId, blockIndex);
                        auto block = options.BlockCache->FindBlock(blockId, EBlockType::CompressedData).Block;
                        blocks.push_back(block);
                        options.ChunkReaderStatistics->DataBytesReadFromCache += block.Size();
                    }
                }
                return MakeFuture(blocks);
            }

            return chunk->ReadBlockSet(blockIndexes, options);
        } catch (const std::exception& ex) {
            return MakeFuture<std::vector<TBlock>>(TError(ex));
        }
    }

private:
    TBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

IChunkBlockManagerPtr CreateChunkBlockManager(TBootstrap* bootstrap)
{
    return New<TChunkBlockManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
