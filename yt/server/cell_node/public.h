#pragma once

#include <core/misc/public.h>

#include <server/misc/public.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

class TCellNodeConfig;
typedef TIntrusivePtr<TCellNodeConfig> TCellNodeConfigPtr;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMemoryCategory,
    (Footprint)
    (BlockCache)
    (ChunkMeta)
    (Job)
    (TabletStatic)
    (TabletDynamic)
);

using TNodeMemoryTracker = TMemoryUsageTracker<EMemoryCategory>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
