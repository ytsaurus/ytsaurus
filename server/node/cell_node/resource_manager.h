#pragma once

#include "public.h"

#include <yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/public.h>

namespace NYT::NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TNodeResourceManager
    : public TRefCounted
{
public:
    TNodeResourceManager(
        IInvokerPtr invoker,
        TBootstrap* bootstrap,
        TDuration updatePeriod);

    void Start();

    double GetJobsCpuLimit() const;

    void OnInstanceLimitsUpdated(double cpuLimit, i64 memoryLimit);

    // TODO(gritukan): Drop it in favour of dynamic config.
    void SetResourceLimitsOverride(const NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides& resourceLimitsOverride);

private:
    const IInvokerPtr Invoker_;
    TBootstrap* const Bootstrap_;
    const TResourceLimitsConfigPtr Config_;

    const NConcurrency::TPeriodicExecutorPtr UpdateExecutor_;

    std::optional<double> TotalCpu_;
    i64 TotalMemory_ = 0;

    double JobsCpuLimit_ = 0;

    NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides ResourceLimitsOverride_;

    void UpdateLimits();
    void UpdateMemoryLimits();
    void UpdateMemoryFootprint();
    void UpdateJobsCpuLimit();
};

DEFINE_REFCOUNTED_TYPE(TNodeResourceManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellNode
