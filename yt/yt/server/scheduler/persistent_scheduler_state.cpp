#include "persistent_scheduler_state.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TPersistentPoolState::Register(TRegistrar registrar)
{
    registrar.Parameter("accumulated_resource_volume", &TThis::AccumulatedResourceVolume)
        .Default({});
}

TString ToString(const TPersistentPoolStatePtr& state)
{
    return ToStringViaBuilder(state);
}

void FormatValue(TStringBuilderBase* builder, const TPersistentPoolStatePtr& state, TStringBuf /*format*/)
{
    builder->AppendFormat("{AccumulatedResourceVolume: %v}", state->AccumulatedResourceVolume);
}

////////////////////////////////////////////////////////////////////////////////

void TPersistentTreeState::Register(TRegistrar registrar)
{
    registrar.Parameter("pool_states", &TThis::PoolStates)
        .Default();

    registrar.Parameter("allocation_scheduler_state", &TThis::AllocationSchedulerState)
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

} // namespace NYT::NScheduler
