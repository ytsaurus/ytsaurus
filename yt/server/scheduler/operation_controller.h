#pragma once

#include "job.h"

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/server/controller_agent/operation_controller.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IOperationController
    : public NControllerAgent::IOperationControllerStrategyHost
{
    //! Called in the end of heartbeat when scheduler agrees to run operation job.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobStarted(const TJobPtr& job) = 0;

    //! Called during heartbeat processing to notify the controller that a job has completed.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobCompleted(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status,
        bool abandoned) = 0;

    //! Called during heartbeat processing to notify the controller that a job has failed.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobFailed(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) = 0;

    //! Called during heartbeat processing to notify the controller that a job has been aborted.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobAborted(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) = 0;

    //! Called during heartbeat processing to notify the controller that a job is still running.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobRunning(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) = 0;

    // XXX(ignat): it is temporary methods.
    virtual NControllerAgent::IOperationControllerPtr GetAgentController() const = 0;
    virtual void SetAgentController(const NControllerAgent::IOperationControllerPtr& controller) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
