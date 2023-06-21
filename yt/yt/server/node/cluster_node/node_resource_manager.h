#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/ytree/ypath_service.h>

#include <yt/yt/library/containers/public.h>

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
    std::optional<double> GetCpuGuarantee() const;
    std::optional<double> GetCpuLimit() const;
    double GetJobsCpuLimit() const;
    double GetTabletSlotCpu() const;
    double GetNodeDedicatedCpu() const;

    double GetCpuUsage() const;
    i64 GetMemoryUsage() const;

    double GetCpuDemand() const;
    i64 GetMemoryDemand() const;

    std::optional<i64> GetNetTxLimit() const;
    std::optional<i64> GetNetRxLimit() const;

    // TODO(gritukan): Drop it in favour of dynamic config.
    void SetResourceLimitsOverride(const NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides& resourceLimitsOverride);

    void OnInstanceLimitsUpdated(const NContainers::TInstanceLimits& limits);

    NYTree::IYPathServicePtr GetOrchidService();

    DEFINE_SIGNAL(void(), JobsCpuLimitUpdated);

    DEFINE_SIGNAL(void(i64), SelfMemoryGuaranteeUpdated);

private:
    IBootstrap* const Bootstrap_;

    const NConcurrency::TPeriodicExecutorPtr UpdateExecutor_;

    TAtomicObject<NContainers::TInstanceLimits> Limits_;

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
