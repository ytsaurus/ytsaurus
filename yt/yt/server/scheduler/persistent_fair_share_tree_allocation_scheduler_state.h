#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TPersistentNodeSchedulingSegmentState
{
    ESchedulingSegment Segment;

    // NB: Used only for diagnostics.
    std::string Address;
};

void FormatValue(TStringBuilderBase* builder, const TPersistentNodeSchedulingSegmentState& state, TStringBuf /*format*/);

// TODO(ehcherbin): Add a new yson struct macro to use TYsonStructLite with public ctor instead of custom serialization.
void Serialize(const TPersistentNodeSchedulingSegmentState& state, NYson::IYsonConsumer* consumer);
void Deserialize(TPersistentNodeSchedulingSegmentState& state, NYTree::INodePtr node);
void Deserialize(TPersistentNodeSchedulingSegmentState& state, NYson::TYsonPullParserCursor* cursor);

using TPersistentNodeSchedulingSegmentStateMap = THashMap<NNodeTrackerClient::TNodeId, TPersistentNodeSchedulingSegmentState>;

////////////////////////////////////////////////////////////////////////////////

struct TPersistentOperationSchedulingSegmentState
{
    std::optional<std::string> Module;
};

void Serialize(const TPersistentOperationSchedulingSegmentState& state, NYson::IYsonConsumer* consumer);
void Deserialize(TPersistentOperationSchedulingSegmentState& state, NYTree::INodePtr node);
void Deserialize(TPersistentOperationSchedulingSegmentState& state, NYson::TYsonPullParserCursor* cursor);

using TPersistentOperationSchedulingSegmentStateMap = THashMap<TOperationId, TPersistentOperationSchedulingSegmentState>;

////////////////////////////////////////////////////////////////////////////////

// COMPAT(eshcherbin): Move to a more suitable file when old scheduling segments state is not used.
struct TPersistentSchedulingSegmentsState
    : public NYTree::TYsonStruct
{
    TPersistentOperationSchedulingSegmentStateMap OperationStates;

    TPersistentNodeSchedulingSegmentStateMap NodeStates;

    REGISTER_YSON_STRUCT(TPersistentSchedulingSegmentsState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPersistentSchedulingSegmentsState)

////////////////////////////////////////////////////////////////////////////////

struct TPersistentFairShareTreeAllocationSchedulerState
    : public NYTree::TYsonStruct
{
    TPersistentSchedulingSegmentsStatePtr SchedulingSegmentsState;

    REGISTER_YSON_STRUCT(TPersistentFairShareTreeAllocationSchedulerState);

    static void Register(TRegistrar registrar);
};

using TPersistentFairShareTreeAllocationSchedulerStatePtr = TIntrusivePtr<TPersistentFairShareTreeAllocationSchedulerState>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
