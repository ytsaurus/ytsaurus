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

class TPersistentTreeState
    : public NYTree::TYsonStruct
{
public:
    THashMap<TString, TPersistentPoolStatePtr> PoolStates;

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

struct TPersistentNodeSchedulingSegmentState
{
    ESchedulingSegment Segment;

    // NB: Used only for diagnostics.
    TString Address;
    TString Tree;
};

void Serialize(const TPersistentNodeSchedulingSegmentState& state, NYson::IYsonConsumer* consumer);
void Deserialize(TPersistentNodeSchedulingSegmentState& state, NYTree::INodePtr node);
void Deserialize(TPersistentNodeSchedulingSegmentState& state, NYson::TYsonPullParserCursor* cursor);

using TPersistentNodeSchedulingSegmentStateMap = THashMap<NNodeTrackerClient::TNodeId, TPersistentNodeSchedulingSegmentState>;

////////////////////////////////////////////////////////////////////////////////

class TPersistentSchedulingSegmentsState
    : public NYTree::TYsonStruct
{
public:
    TPersistentNodeSchedulingSegmentStateMap NodeStates;

    REGISTER_YSON_STRUCT(TPersistentSchedulingSegmentsState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPersistentSchedulingSegmentsState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
