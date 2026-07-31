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

    std::optional<TNetworkPriority> NetworkPriority;

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

// COMPAT(bystrovserg): Converts the classic policy's persistent state into the GPU policy format.
NYTree::INodePtr ConvertClassicToGpuPersistentState(const NYTree::INodePtr& node);

// COMPAT(bystrovserg): Converts the GPU policy's persistent state into the classic policy format.
NYTree::INodePtr ConvertGpuToClassicPersistentState(const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
