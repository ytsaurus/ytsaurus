#include "persistent_fair_share_tree_allocation_scheduler_state.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TPersistentNodeSchedulingSegmentState& state, TStringBuf /*format*/)
{
    builder->AppendFormat("{Segment: %v, Address: %v}",
        state.Segment,
        state.Address);
}

void Serialize(const TPersistentNodeSchedulingSegmentState& state, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("segment").Value(state.Segment)
            .Item("address").Value(state.Address)
        .EndMap();
}

void Deserialize(TPersistentNodeSchedulingSegmentState& state, INodePtr node)
{
    auto mapNode = node->AsMap();
    Deserialize(state.Segment, mapNode->GetChildOrThrow("segment"));
    Deserialize(state.Address, mapNode->GetChildOrThrow("address"));
}

void Deserialize(TPersistentNodeSchedulingSegmentState& state, NYson::TYsonPullParserCursor* cursor)
{
    Deserialize(state, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TPersistentOperationSchedulingSegmentState& state, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("module").Value(state.Module)
        .EndMap();
}

void Deserialize(TPersistentOperationSchedulingSegmentState& state, INodePtr node)
{
    auto mapNode = node->AsMap();
    Deserialize(state.Module, mapNode->GetChildOrThrow("module"));
}

void Deserialize(TPersistentOperationSchedulingSegmentState& state, NYson::TYsonPullParserCursor* cursor)
{
    Deserialize(state, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

void TPersistentSchedulingSegmentsState::Register(TRegistrar registrar)
{
    registrar.Parameter("node_states", &TThis::NodeStates)
        .Default();
    registrar.Parameter("operation_states", &TThis::OperationStates)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TPersistentFairShareTreeAllocationSchedulerState::Register(TRegistrar registrar)
{
    registrar.Parameter("scheduling_segments_state", &TThis::SchedulingSegmentsState)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
