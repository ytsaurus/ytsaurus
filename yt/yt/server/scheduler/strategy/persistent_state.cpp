#include "persistent_state.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler::NStrategy {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TPersistentPoolState::Register(TRegistrar registrar)
{
    registrar.Parameter("accumulated_resource_volume", &TThis::AccumulatedResourceVolume)
        .Default({});
}

void FormatValue(TStringBuilderBase* builder, const TPersistentPoolStatePtr& state, TStringBuf /*spec*/)
{
    builder->AppendFormat("{AccumulatedResourceVolume: %v}", state->AccumulatedResourceVolume);
}

////////////////////////////////////////////////////////////////////////////////

void TPersistentTreeState::Register(TRegistrar registrar)
{
    registrar.Parameter("pool_states", &TThis::PoolStates)
        .Default();

    registrar.Parameter("scheduling_policy_state", &TThis::SchedulingPolicyState)
        .Alias("allocation_scheduler_state")
        .Alias("job_scheduler_state")
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TPersistentStrategyState::Register(TRegistrar registrar)
{
    registrar.Parameter("tree_states", &TThis::TreeStates)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
