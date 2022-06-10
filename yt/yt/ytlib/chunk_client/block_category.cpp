#include "block_category.h"
#include "block.h"

#include <yt/yt/ytlib/memory_trackers/block_tracker.h>

namespace NYT::NChunkClient {

/////////////////////////////////////////////////////////////////////////////

TBlock ResetCategory(
    TBlock block,
    IBlockTrackerPtr tracker,
    std::optional<EMemoryCategory> category)
{
    block.Data = ResetCategory(
        std::move(block.Data),
        std::move(tracker),
        category);

    return block;
}

/////////////////////////////////////////////////////////////////////////////

TBlock AttachCategory(
    TBlock block,
    IBlockTrackerPtr tracker,
    std::optional<EMemoryCategory> category)
{
    block.Data = AttachCategory(std::move(block.Data), std::move(tracker), category);

    return block;
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
