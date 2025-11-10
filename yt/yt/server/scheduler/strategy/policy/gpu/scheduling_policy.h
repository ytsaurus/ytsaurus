#pragma once

#include "public.h"
#include "structs.h"
#include "assignment_plan_update.h"
#include "assignment_plan_context_detail.h"

#include <yt/yt/server/scheduler/strategy/policy/public.h>

#include <yt/yt/server/scheduler/strategy/public.h>

#include <yt/yt/server/lib/scheduler/public.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

struct ISchedulingPolicy
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    //! Node management.
    virtual void RegisterNode(NNodeTrackerClient::TNodeId nodeId, const std::string& nodeAddress) = 0;
    virtual void UnregisterNode(NNodeTrackerClient::TNodeId nodeId) = 0;

    virtual void UpdateNodeDescriptor(NNodeTrackerClient::TNodeId nodeId, TExecNodeDescriptorPtr descriptor) = 0;

    //! Operation management.
    virtual void RegisterOperation(const TPoolTreeOperationElement* element) = 0;
    virtual void UnregisterOperation(const TPoolTreeOperationElement* element) = 0;

    virtual void OnOperationMaterialized(const TPoolTreeOperationElement* element) = 0;

    virtual void EnableOperation(const TPoolTreeOperationElement* element) = 0;
    virtual void DisableOperation(const TPoolTreeOperationElement* element) = 0;

    //! Diagnostics.
    virtual void PopulateOrchidService(const NYTree::TCompositeMapServicePtr& orchidService) const = 0;

    //! Miscellaneous.
    virtual void UpdateConfig(TGpuSchedulingPolicyConfigPtr config) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchedulingPolicy)

////////////////////////////////////////////////////////////////////////////////

ISchedulingPolicyPtr CreateSchedulingPolicy(
    TWeakPtr<ISchedulingPolicyHost> host,
    IStrategyHost* strategyHost,
    const std::string& treeId,
    const TStrategyTreeConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
