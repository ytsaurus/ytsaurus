#pragma once

#include "job.h"

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IOperationController
    : public virtual TRefCounted
{
    //! Called in the end of heartbeat when scheduler agrees to run operation job.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobStarted(const TJobId& jobId, TInstant startTime) = 0;

    //! Called during heartbeat processing to notify the controller that a job has completed.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobCompleted(
        const TJobPtr& job,
        const NJobTrackerClient::NProto::TJobStatus& status,
        bool abandoned) = 0;

    //! Called during heartbeat processing to notify the controller that a job has failed.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobFailed(
        const TJobPtr& job,
        const NJobTrackerClient::NProto::TJobStatus& status) = 0;

    //! Called during heartheat processing to notify the controller that a job has been aborted.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobAborted(
        const TJobPtr& job,
        const NJobTrackerClient::NProto::TJobStatus& status) = 0;

    //! Called during scheduling to notify the controller that a job has been aborted.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobAborted(
        const TJobPtr& job,
        EAbortReason abortReason) = 0;

    //! Called during heartbeat processing to notify the controller that a job is still running.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobRunning(
        const TJobPtr& job,
        const NJobTrackerClient::NProto::TJobStatus& status) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
