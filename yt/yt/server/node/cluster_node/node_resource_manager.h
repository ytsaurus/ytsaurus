#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/ytree/ypath_service.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

class TNodeResourceManager
    : public TRefCounted
{
public:
    explicit TNodeResourceManager(IBootstrap* bootstrap);

    void Start();

    /*!
    *  \note
    *  Thread affinity: any
    */
    std::optional<double> GetCpuLimit() const;
    double GetJobsCpuLimit() const;
    double GetTabletSlotCpu() const;
    double GetNodeDedicatedCpu() const;

    double GetCpuUsage() const;
    i64 GetMemoryUsage() const;

    double GetCpuDemand() const;
    i64 GetMemoryDemand() const;

    // TODO(gritukan): Drop it in favour of dynamic config.
    void SetResourceLimitsOverride(const NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides& resourceLimitsOverride);

    void OnInstanceLimitsUpdated(double cpuLimit, i64 memoryLimit);
    
    NYTree::IYPathServicePtr GetOrchidService();

    DEFINE_SIGNAL(void(), JobsCpuLimitUpdated);

    DEFINE_SIGNAL(void(i64), SelfMemoryGuaranteeUpdated);

private:
    IBootstrap* const Bootstrap_;

    const NConcurrency::TPeriodicExecutorPtr UpdateExecutor_;

    std::optional<double> TotalCpu_;
    i64 TotalMemory_ = 0;

    i64 SelfMemoryGuarantee_ = 0;
    std::atomic<double> JobsCpuLimit_ = 0;

    NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides ResourceLimitsOverride_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void UpdateLimits();
    void UpdateMemoryLimits();
    void UpdateMemoryFootprint();
    void UpdateJobsCpuLimit();

    NNodeTrackerClient::NProto::TNodeResources GetJobResourceUsage() const;

    void BuildOrchid(NYson::IYsonConsumer* consumer) const;

    TEnumIndexedVector<EMemoryCategory, TMemoryLimitPtr> GetMemoryLimits() const;
};

DEFINE_REFCOUNTED_TYPE(TNodeResourceManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
