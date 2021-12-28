#include "chunk_registry.h"

#include "bootstrap.h"
#include "chunk.h"
#include "chunk_store.h"
#include "location.h"

#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/chunk_cache.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/mpsc_stack.h>
#include <yt/yt/core/misc/ring_queue.h>

namespace NYT::NDataNode {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

class TChunkRegistry
    : public IChunkRegistry
{
public:
    explicit TChunkRegistry(IBootstrapBase* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    IChunkPtr FindChunk(
        TChunkId chunkId,
        int mediumIndex) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // There are two possible places where we can look for a chunk: ChunkStore and ChunkCache.
        if (Bootstrap_->IsDataNode()) {
            const auto& chunkStore = Bootstrap_->GetDataNodeBootstrap()->GetChunkStore();
            if (auto storedChunk = chunkStore->FindChunk(chunkId, mediumIndex)) {
                return storedChunk;
            }
        }

        if (mediumIndex != AllMediaIndex) {
            return nullptr;
        }

        if (Bootstrap_->IsExecNode()) {
            const auto& chunkCache = Bootstrap_->GetExecNodeBootstrap()->GetChunkCache();
            if (auto cachedChunk = chunkCache->FindChunk(chunkId)) {
                return cachedChunk;
            }
        }

        return nullptr;
    }

    IChunkPtr GetChunkOrThrow(
        TChunkId chunkId,
        int mediumIndex) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto chunk = FindChunk(chunkId, mediumIndex);
        if (!chunk) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchChunk,
                "No such chunk %v",
                chunkId);
        }

        return chunk;
    }

private:
    IBootstrapBase* const Bootstrap_;
};

IChunkRegistryPtr CreateChunkRegistry(IBootstrapBase* bootstrap)
{
    return New<TChunkRegistry>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
