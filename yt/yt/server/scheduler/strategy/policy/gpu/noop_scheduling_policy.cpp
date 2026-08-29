#include "noop_scheduling_policy.h"

#include <yt/yt/server/scheduler/common/allocation.h>

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

using namespace NNodeTrackerClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TNoopSchedulingPolicy
    : public ISchedulingPolicy
{
public:
    explicit TNoopSchedulingPolicy(NLogging::TLogger logger)
        : Logger(std::move(logger))
    { }

    void Initialize() override
    { }

    void RegisterNode(TNodeId /*nodeId*/, const std::string& /*nodeAddress*/) override
    { }

    void UnregisterNode(TNodeId /*nodeId*/) override
    { }

    TFuture<void> ProcessSchedulingHeartbeat(
        const ISchedulingHeartbeatContextPtr& /*schedulingHeartbeatContext*/,
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        bool /*skipScheduleAllocations*/) override
    {
        return OKFuture;
    }

    void RegisterOperation(const TPoolTreeOperationElement* /*element*/) override
    { }

    void UnregisterOperation(const TPoolTreeOperationElement* /*element*/) override
    { }

    TError OnOperationMaterialized(const TPoolTreeOperationElement* /*element*/, bool /*revivedFromSnapshot*/) override
    {
        return {};
    }

    TError CheckOperationSchedulingInSeveralTreesAllowed(const TPoolTreeOperationElement* /*element*/) const override
    {
        return {};
    }

    void EnableOperation(const TPoolTreeOperationElement* /*element*/) override
    { }

    void DisableOperation(TPoolTreeOperationElement* /*element*/, bool /*markAsNonAlive*/) override
    { }

    void RegisterAllocationsFromRevivedOperation(
        TPoolTreeOperationElement* /*element*/,
        std::vector<TAllocationPtr> /*allocations*/) override
    { }

    TFuture<std::vector<TProcessAllocationUpdateResult>> ProcessAllocationUpdates(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        const std::vector<TAllocationUpdate>& /*allocationUpdates*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void BuildSchedulingAttributesStringForNode(
        const ISchedulingHeartbeatContextPtr& /*schedulingHeartbeatContext*/,
        TNodeId /*nodeId*/,
        TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const override
    { }

    void BuildSchedulingAttributesForNode(TNodeId /*nodeId*/, TFluentMap /*fluent*/) const override
    { }

    NLogging::TLoggingTagList BuildSchedulingAttributeTagsForOngoingAllocations(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        const std::vector<TAllocationPtr>& /*allocations*/,
        TInstant /*now*/) const override
    {
        return {};
    }

    NLogging::TLoggingTagList BuildElementLoggingTags(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        const TPoolTreeElement* /*element*/) const override
    {
        return {};
    }

    void PopulateOrchidService(const ICompositeMapServicePtr& /*orchidService*/) const override
    { }

    void ProfileOperation(
        const TPoolTreeOperationElement* /*element*/,
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        NProfiling::ISensorWriter* /*writer*/) const override
    { }

    TPostUpdateContextPtr CreatePostUpdateContext(TPoolTreeRootElement* /*rootElement*/) override
    {
        return nullptr;
    }

    void PostUpdate(
        TFairSharePostUpdateContext* /*fairSharePostUpdateContext*/,
        TPostUpdateContextPtr* /*postUpdateContext*/) override
    { }

    TPoolTreeSnapshotStatePtr CreateSnapshotState(TPostUpdateContextPtr* /*postUpdateContext*/) override
    {
        return nullptr;
    }

    void OnResourceUsageSnapshotUpdate(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        const TResourceUsageSnapshotPtr& /*resourceUsageSnapshot*/) const override
    { }

    void UpdateConfig(TStrategyTreeConfigPtr config) override
    {
        if (EGpuSchedulingPolicyMode::Noop != config->GpuSchedulingPolicy->Mode) {
            YT_TLOG_WARNING("GPU scheduling policy config update failed because mode has changed")
                .With("OldMode", EGpuSchedulingPolicyMode::Noop)
                .With("NewMode", config->GpuSchedulingPolicy->Mode);
            return;
        }
    }

    void InitPersistentState(INodePtr /*persistentState*/) override
    { }

    INodePtr BuildPersistentState() const override
    {
        return {};
    }

private:
    const NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TNoopSchedulingPolicy)

////////////////////////////////////////////////////////////////////////////////

ISchedulingPolicyPtr CreateNoopSchedulingPolicy(NLogging::TLogger logger)
{
    return New<TNoopSchedulingPolicy>(std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
