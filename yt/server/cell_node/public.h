#pragma once

#include "memory_consumer.h"

#include <core/misc/public.h>

#include <server/misc/memory_usage_tracker.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

class TCellNodeConfig;
typedef TIntrusivePtr<TCellNodeConfig> TCellNodeConfigPtr;

typedef TMemoryUsageTracker<EMemoryConsumer> TNodeMemoryTracker;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
