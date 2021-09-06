#pragma once

#include "public.h"
#include "job.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/node/exec_node/job.h>

#include <yt/yt/server/job_proxy/public.h>

#include <yt/yt/ytlib/job_tracker_client/proto/job_tracker_service.pb.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/profiling/profile_manager.h>

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
 *   \note Thread affinity: any (unless noted otherwise)
 */
class TJobController
    : public TRefCounted
{
public:
    TJobController(
        TJobControllerConfigPtr config,
        NClusterNode::IBootstrapBase* bootstrap);

    ~TJobController();

    void Initialize();

    //! Registers a factory for a given scheduer job type.
    void RegisterSchedulerJobFactory(
        EJobType type,
        TSchedulerJobFactory factory);
    
    //! Registers a factory for a given master job type.
    void RegisterMasterJobFactory(
        EJobType type,
        TMasterJobFactory factory);

    //! Finds the job by its id, returns |nullptr| if no job is found.
    /*
     * \note Thread affinity: any
     */
    IJobPtr FindJob(TJobId jobId) const;

    //! Finds the job by its id, throws if no job is found.
    IJobPtr GetJobOrThrow(TJobId jobId) const;

    //! Returns the list of all currently known jobs.
    std::vector<IJobPtr> GetJobs() const;

    //! Finds the job that is held after it has been removed.
    IJobPtr FindRecentlyRemovedJob(TJobId jobId) const;

    //! Returns the maximum allowed resource usage.
    NNodeTrackerClient::NProto::TNodeResources GetResourceLimits() const;

    //! Checks dynamic config to see if job proxy profiling is disabled.
    bool IsJobProxyProfilingDisabled() const;

    //! Returns dynamic config of job proxy.
    NJobProxy::TJobProxyDynamicConfigPtr GetJobProxyDynamicConfig() const;

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
    TFuture<void> PrepareHeartbeatRequest(
        NObjectClient::TCellTag cellTag,
        NObjectClient::EObjectType jobObjectType,
        const TReqHeartbeatPtr& request);

    //! Handles heartbeat response, i.e. starts new jobs, aborts and removes old ones etc.
    TFuture<void> ProcessHeartbeatResponse(
        const TRspHeartbeatPtr& response,
        NObjectClient::EObjectType jobObjectType);

    NYTree::IYPathServicePtr GetOrchidService();

    DECLARE_SIGNAL(void(), ResourcesUpdated);
    DECLARE_SIGNAL(void(const IJobPtr&), JobFinished);
    DECLARE_SIGNAL(void(const TError& error), JobProxyBuildInfoUpdated);

    class TJobHeartbeatProcessorBase
        : public TRefCounted
    {
    public:
        TJobHeartbeatProcessorBase(
            TJobController* controller,
            NClusterNode::IBootstrapBase* bootstrap);

        virtual void PrepareRequest(
            NObjectClient::TCellTag cellTag,
            const TReqHeartbeatPtr& request) = 0;
        virtual void ProcessResponse(
            const TRspHeartbeatPtr& response) = 0;
    protected:
        void RemoveSchedulerJobsOnFatalAlert();
        bool NeedTotalConfirmation();
        TFuture<void> RequestJobSpecsAndStartJobs(
            std::vector<NJobTrackerClient::NProto::TJobStartInfo> jobStartInfos);
        IJobPtr CreateMasterJob(
            TJobId jobId,
            TOperationId operationId,
            const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
            NJobTrackerClient::NProto::TJobSpec&& jobSpec);
        const THashMap<TJobId, TOperationId>& GetSpecFetchFailedJobIds();
        bool StatisticsThrottlerTryAcquire(int size);

        void PrepareHeartbeatCommonRequestPart(const TReqHeartbeatPtr& request);
        void ProcessHeartbeatCommonResponsePart(const TRspHeartbeatPtr& response);

        TErrorOr<NExecNode::TControllerAgentDescriptor> TryParseControllerAgentDescriptor(
            const NJobTrackerClient::NProto::TControllerAgentDescriptor& proto) const;

        TJobController* const JobController_;
        NClusterNode::IBootstrapBase* const Bootstrap_;
    };

    using TJobHeartbeatProcessorBasePtr = TIntrusivePtr<TJobHeartbeatProcessorBase>;

    friend class TJobHeartbeatProcessorBase;

    template <typename T>
    void AddHeartbeatProcessor(
        NObjectClient::EObjectType jobObjectType,
        NClusterNode::IBootstrapBase* const bootstrap)
    {
        auto heartbeatProcessor = New<T>(this, bootstrap);
        RegisterHeartbeatProcessor(jobObjectType, std::move(heartbeatProcessor));
    }

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

    void RegisterHeartbeatProcessor(
        NObjectClient::EObjectType type,
        TJobHeartbeatProcessorBasePtr heartbeatProcessor);
};

DEFINE_REFCOUNTED_TYPE(TJobController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
