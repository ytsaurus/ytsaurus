#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NCellScheduler {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

struct TCellSchedulerConfig;
typedef TIntrusivePtr<TCellSchedulerConfig> TCellSchedulerConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
