#include "persistent_fair_share_tree_job_scheduler_state.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

void TPersistentFairShareTreeJobSchedulerState::Register(TRegistrar registrar)
{
    registrar.Parameter("scheduling_segments_state", &TThis::SchedulingSegmentsState)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
