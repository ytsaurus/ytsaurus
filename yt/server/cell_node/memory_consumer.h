#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/enum.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EMemoryConsumer,
    (Footprint)
    (BlockCache)
    (ChunkMeta)
    (Job)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT