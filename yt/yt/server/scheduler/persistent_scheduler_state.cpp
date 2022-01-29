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

void FormatValue(TStringBuilderBase* builder, const TPersistentPoolStatePtr& state, TStringBuf /* format */)
{
    builder->AppendFormat("{AccumulatedResourceVolume: %v}", state->AccumulatedResourceVolume);
}

////////////////////////////////////////////////////////////////////////////////

void TPersistentTreeState::Register(TRegistrar registrar)
{
    registrar.Parameter("pool_states", &TThis::PoolStates)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TPersistentStrategyState::Register(TRegistrar registrar)
{
    registrar.Parameter("tree_states", &TThis::TreeStates)
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

void Deserialize(TPersistentNodeSchedulingSegmentState& state, NYson::TYsonPullParserCursor* cursor)
{
    Deserialize(state, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

void TPersistentSchedulingSegmentsState::Register(TRegistrar registrar)
{
    registrar.Parameter("node_states", &TThis::NodeStates)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
