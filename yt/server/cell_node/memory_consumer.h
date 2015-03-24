#pragma once

#include <core/misc/enum.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMemoryConsumer,
    (Footprint)
    (BlockCache)
    (ChunkMeta)
    (Job)
    (TabletStatic)
    (TabletDynamic)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
