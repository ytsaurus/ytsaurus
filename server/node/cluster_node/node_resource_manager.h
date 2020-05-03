#pragma once

#include "public.h"

#include <yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/thread_affinity.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

class TNodeResourceManager
    : public TRefCounted
{
public:
    explicit TNodeResourceManager(TBootstrap* bootstrap);

    void Start();

    /*!
    *  \note
    *  Thread affinity: any
    */
    double GetJobsCpuLimit() const;

    // TODO(gritukan): Drop it in favour of dynamic config.
    void SetResourceLimitsOverride(const NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides& resourceLimitsOverride);

    DEFINE_SIGNAL(void(), JobsCpuLimitUpdated);

private:
    TBootstrap* const Bootstrap_;
    const TResourceLimitsConfigPtr Config_;

    const NConcurrency::TPeriodicExecutorPtr UpdateExecutor_;

    std::optional<double> TotalCpu_;
    i64 TotalMemory_ = 0;

    std::atomic<double> JobsCpuLimit_ = 0;

    NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides ResourceLimitsOverride_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void OnInstanceLimitsUpdated(double cpuLimit, i64 memoryLimit);

    void UpdateLimits();
    void UpdateMemoryLimits();
    void UpdateMemoryFootprint();
    void UpdateJobsCpuLimit();
};

DEFINE_REFCOUNTED_TYPE(TNodeResourceManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
