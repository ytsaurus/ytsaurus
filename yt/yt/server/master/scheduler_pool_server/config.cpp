#include "config.h"

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

void TDynamicSchedulerPoolManagerConfig::Register(TRegistrar registrar) {
    registrar.Parameter("max_scheduler_pool_subtree_size", &TThis::MaxSchedulerPoolSubtreeSize)
        .Default(1000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
