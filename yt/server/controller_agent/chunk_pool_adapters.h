#include "private.h"

#include "task_host.h"

#include <yt/server/chunk_pools/chunk_pool.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NChunkPools::IChunkPoolInput> CreateIntermediateLivePreviewAdapter(
    NChunkPools::IChunkPoolInput* chunkPoolInput,
    ITaskHost* taskHost);

std::unique_ptr<NChunkPools::IChunkPoolInput> CreateHintAddingAdapter(
    NChunkPools::IChunkPoolInput* chunkPoolInput,
    TTask* task);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT