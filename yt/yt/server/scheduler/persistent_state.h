#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>
#include <yt/yt/library/vector_hdrf/resource_volume.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TPersistentPoolState
    : public NYTree::TYsonStruct  // TODO(renadeen): Try to make it lite.
{
    TResourceVolume AccumulatedResourceVolume;

    REGISTER_YSON_STRUCT(TPersistentPoolState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPersistentPoolState)

void FormatValue(TStringBuilderBase* builder, const TPersistentPoolStatePtr& state, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

struct TPersistentTreeState
    : public NYTree::TYsonStruct
{
    THashMap<TString, TPersistentPoolStatePtr> PoolStates;

    NYTree::INodePtr AllocationSchedulerState;

    REGISTER_YSON_STRUCT(TPersistentTreeState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPersistentTreeState)

////////////////////////////////////////////////////////////////////////////////

struct TPersistentStrategyState
    : public NYTree::TYsonStruct
{
    THashMap<TString, TPersistentTreeStatePtr> TreeStates;

    REGISTER_YSON_STRUCT(TPersistentStrategyState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPersistentStrategyState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
