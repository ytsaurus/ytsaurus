#include "persistent_pool_state.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TPersistentPoolState::TPersistentPoolState()
{
    RegisterParameter("accumulated_resource_volume", AccumulatedResourceVolume)
        .Default(0.0);
}

TString ToString(const TPersistentPoolStatePtr& state)
{
    return ToStringViaBuilder(state);
}

void FormatValue(TStringBuilderBase* builder, const TPersistentPoolStatePtr& state, TStringBuf /* format */)
{
    builder->AppendFormat("{AccumulatedResourceVolume: %v}", state->AccumulatedResourceVolume);
}

////////////////////////////////////////////////////////////////////////////////

TPersistentTreeState::TPersistentTreeState()
{
    RegisterParameter("pool_states", PoolStates)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TPersistentStrategyState::TPersistentStrategyState()
{
    RegisterParameter("tree_states", TreeStates)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
