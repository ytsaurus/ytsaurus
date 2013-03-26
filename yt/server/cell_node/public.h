#pragma once

#include "memory_consumer.h"

#include <ytlib/misc/common.h>

#include <server/misc/memory_usage_tracker.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

struct TCellNodeConfig;
typedef TIntrusivePtr<TCellNodeConfig> TCellNodeConfigPtr;

typedef TMemoryUsageTracker<EMemoryConsumer> TNodeMemoryTracker;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
