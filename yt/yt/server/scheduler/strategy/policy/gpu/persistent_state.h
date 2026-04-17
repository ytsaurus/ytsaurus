#pragma once

#include "structs.h"

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

struct TPersistentOperationState
    : public NYTree::TYsonStructLite
{
    std::optional<std::string> SchedulingModule;

    REGISTER_YSON_STRUCT_LITE(TPersistentOperationState);

    static void Register(TRegistrar registrar);
};

using TPersistentOperationStateMap = THashMap<TOperationId, TPersistentOperationState>;

////////////////////////////////////////////////////////////////////////////////

struct TPersistentNodeState
    : public NYTree::TYsonStructLite
{
    std::optional<std::string> SchedulingModule;

    // For debug purposes.
    std::string Address;

    REGISTER_YSON_STRUCT_LITE(TPersistentNodeState);

    static void Register(TRegistrar registrar);
};

using TPersistentNodeStateMap = THashMap<NNodeTrackerClient::TNodeId, TPersistentNodeState>;

////////////////////////////////////////////////////////////////////////////////

struct TPersistentState
    : public NYTree::TYsonStruct
{
    TPersistentNodeStateMap NodeStates;
    TPersistentOperationStateMap OperationStates;

    REGISTER_YSON_STRUCT(TPersistentState);

    static void Register(TRegistrar registrar);
};

using TPersistentStatePtr = TIntrusivePtr<TPersistentState>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
