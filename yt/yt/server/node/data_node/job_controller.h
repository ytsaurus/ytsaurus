#pragma once

#include "public.h"
#include "job.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/node/job_agent/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Controls all master jobs scheduled to run at this node.
/*!
 *   Maintains a map of jobs, allows new jobs to be started and existing jobs to be stopped.
 *   New jobs are constructed by means of per-type factories registered via #RegisterFactory.
 *
 *   \note Thread affinity: any (unless noted otherwise)
 */
class IJobController
    : public TRefCounted
{
public:
    virtual void Initialize() = 0;

    virtual void ScheduleStartJobs() = 0;

    DECLARE_INTERFACE_SIGNAL(void(const TMasterJobBasePtr&), JobFinished);

    using TRspHeartbeat = NRpc::TTypedClientResponse<
        NJobTrackerClient::NProto::TRspHeartbeat>;
    using TReqHeartbeat = NRpc::TTypedClientRequest<
        NJobTrackerClient::NProto::TReqHeartbeat,
        TRspHeartbeat>;
    using TRspHeartbeatPtr = TIntrusivePtr<TRspHeartbeat>;
    using TReqHeartbeatPtr = TIntrusivePtr<TReqHeartbeat>;

    //! Prepares a heartbeat request.
    virtual TFuture<void> PrepareHeartbeatRequest(
        NObjectClient::TCellTag cellTag,
        const TString& jobTrackerAddress,
        const TReqHeartbeatPtr& request) = 0;

    //! Handles heartbeat response, i.e. starts new jobs, aborts and removes old ones etc.
    virtual TFuture<void> ProcessHeartbeatResponse(
        const TString& jobTrackerAddress,
        const TRspHeartbeatPtr& response) = 0;

    virtual void BuildJobsInfo(NYTree::TFluentAny fluent) const = 0;

    virtual int GetActiveJobCount() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobController)

////////////////////////////////////////////////////////////////////////////////

IJobControllerPtr CreateJobController(NClusterNode::IBootstrapBase* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
