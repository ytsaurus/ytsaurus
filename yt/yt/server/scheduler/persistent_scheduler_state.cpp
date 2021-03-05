#include "persistent_scheduler_state.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TPersistentPoolState::TPersistentPoolState()
{
    RegisterParameter("accumulated_resource_volume", AccumulatedResourceVolume)
        .Default({});
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

void Serialize(const TPersistentNodeSchedulingSegmentState& state, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("segment").Value(state.Segment)
            .Item("address").Value(state.Address)
            .Item("tree").Value(state.Tree)
        .EndMap();
}

void Deserialize(TPersistentNodeSchedulingSegmentState& state, INodePtr node)
{
    auto mapNode = node->AsMap();
    Deserialize(state.Segment, mapNode->GetChildOrThrow("segment"));
    Deserialize(state.Address, mapNode->GetChildOrThrow("address"));
    Deserialize(state.Tree, mapNode->GetChildOrThrow("tree"));
}

////////////////////////////////////////////////////////////////////////////////

TPersistentSchedulingSegmentsState::TPersistentSchedulingSegmentsState()
{
    RegisterParameter("node_states", NodeStates)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
