#pragma once

#include "public.h"
#include "job.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/job_tracker_client/proto/job_spec_service.pb.h>
#include <yt/ytlib/job_tracker_client/job_spec_service_proxy.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJobOrigin,
    ((Master)    (0))
    ((Scheduler) (1))
);

////////////////////////////////////////////////////////////////////////////////

//! Controls all jobs scheduled to run at this node.
/*!
 *   Maintains a map of jobs, allows new jobs to be started and existing jobs to be stopped.
 *   New jobs are constructed by means of per-type factories registered via #RegisterFactory.
 *
 */
class TJobController
    : public TRefCounted
{
public:
    DECLARE_SIGNAL(void(), ResourcesUpdated)

    TJobController(
        TJobControllerConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    void Initialize();

    //! Registers a factory for a given job type.
    void RegisterFactory(
        EJobType type,
        TJobFactory factory);

    //! Finds the job by its id, returns |nullptr| if no job is found.
    IJobPtr FindJob(TJobId jobId) const;

    //! Finds the job by its id, throws if no job is found.
    IJobPtr GetJobOrThrow(TJobId jobId) const;

    //! Returns the list of all currently known jobs.
    std::vector<IJobPtr> GetJobs() const;

    //! Returns the maximum allowed resource usage.
    NNodeTrackerClient::NProto::TNodeResources GetResourceLimits() const;

    //! Return the current resource usage.
    NNodeTrackerClient::NProto::TNodeResources GetResourceUsage(bool includeWaiting = false) const;

    //! Return ports allocated by job.
    std::vector<int> GetJobPorts(TJobId jobId) const;

    //! Set resource limits overrides.
    void SetResourceLimitsOverrides(const NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides& resourceLimits);

    //! Set value of flag disabling all scheduler jobs.
    void SetDisableSchedulerJobs(bool value);

    using TRspHeartbeat = NRpc::TTypedClientResponse<
        NJobTrackerClient::NProto::TRspHeartbeat>;
    using TReqHeartbeat = NRpc::TTypedClientRequest<
        NJobTrackerClient::NProto::TReqHeartbeat,
        TRspHeartbeat>;
    using TRspHeartbeatPtr = TIntrusivePtr<TRspHeartbeat>;
    using TReqHeartbeatPtr = TIntrusivePtr<TReqHeartbeat>;

    //! Prepares a heartbeat request.
    void PrepareHeartbeatRequest(
        NObjectClient::TCellTag cellTag,
        NObjectClient::EObjectType jobObjectType,
        const TReqHeartbeatPtr& request);

    //! Handles heartbeat response, i.e. starts new jobs, aborts and removes old ones etc.
    void ProcessHeartbeatResponse(
        const TRspHeartbeatPtr& response,
        NObjectClient::EObjectType jobObjectType);

    //! Orchid server.
    NYTree::IYPathServicePtr GetOrchidService();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TJobController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
