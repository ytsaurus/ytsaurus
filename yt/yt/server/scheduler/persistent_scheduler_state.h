#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>
#include <yt/yt/library/vector_hdrf/resource_volume.h>
#include <yt/yt/library/vector_hdrf/resource_helpers.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TPersistentPoolState
    : public NYTree::TYsonStruct  // TODO(renadeen): Try to make it lite.
{
public:
    TResourceVolume AccumulatedResourceVolume;

    REGISTER_YSON_STRUCT(TPersistentPoolState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPersistentPoolState)

TString ToString(const TPersistentPoolStatePtr& state);
void FormatValue(TStringBuilderBase* builder, const TPersistentPoolStatePtr& state, TStringBuf /* format */);

////////////////////////////////////////////////////////////////////////////////

struct TPersistentNodeSchedulingSegmentState
{
    ESchedulingSegment Segment;

    // NB: Used only for diagnostics.
    TString Address;
    // COMPAT(eshcherbin): Remove when global scheduling segments state is not used anymore.
    TString Tree;
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
    std::optional<TString> Module;
};

void Serialize(const TPersistentOperationSchedulingSegmentState& state, NYson::IYsonConsumer* consumer);
void Deserialize(TPersistentOperationSchedulingSegmentState& state, NYTree::INodePtr node);
void Deserialize(TPersistentOperationSchedulingSegmentState& state, NYson::TYsonPullParserCursor* cursor);

using TPersistentOperationSchedulingSegmentStateMap = THashMap<TOperationId, TPersistentOperationSchedulingSegmentState>;

////////////////////////////////////////////////////////////////////////////////

// COMPAT(eshcherbin): Move to a more suitable file when old scheduling segments state is not used.
class TPersistentSchedulingSegmentsState
    : public NYTree::TYsonStruct
{
public:
    TPersistentOperationSchedulingSegmentStateMap OperationStates;

    TPersistentNodeSchedulingSegmentStateMap NodeStates;

    REGISTER_YSON_STRUCT(TPersistentSchedulingSegmentsState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPersistentSchedulingSegmentsState)

////////////////////////////////////////////////////////////////////////////////

class TPersistentTreeState
    : public NYTree::TYsonStruct
{
public:
    THashMap<TString, TPersistentPoolStatePtr> PoolStates;

    NYTree::INodePtr JobSchedulerState;

    REGISTER_YSON_STRUCT(TPersistentTreeState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPersistentTreeState)

////////////////////////////////////////////////////////////////////////////////

class TPersistentStrategyState
    : public NYTree::TYsonStruct
{
public:
    THashMap<TString, TPersistentTreeStatePtr> TreeStates;

    REGISTER_YSON_STRUCT(TPersistentStrategyState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPersistentStrategyState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
