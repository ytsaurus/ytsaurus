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
void FormatValue(TStringBuilderBase* builder, const TPersistentPoolStatePtr& state, TStringBuf /*format*/);

////////////////////////////////////////////////////////////////////////////////

class TPersistentTreeState
    : public NYTree::TYsonStruct
{
public:
    THashMap<TString, TPersistentPoolStatePtr> PoolStates;

    NYTree::INodePtr AllocationSchedulerState;

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
