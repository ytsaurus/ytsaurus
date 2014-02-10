#pragma once

#include <core/misc/common.h>
#include <core/misc/enum.h>

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
