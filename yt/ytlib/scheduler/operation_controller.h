#pragma once

#include "public.h"

#include <ytlib/scheduler/jobs.pb.h>
#include <ytlib/rpc/channel.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IOperationHost
{
    virtual ~IOperationHost()
    { }


    virtual NRpc::IChannel::TPtr GetMasterChannel() = 0;

    //! A factory method for creating new jobs.
    /*!
     *  The controller must not instantiate jobs by itself since
     *  this needs some internal bookkeeping from the scheduler
     *  (e.g. id and start time assignment).
     */
    virtual TJobPtr CreateJob(
        TOperationPtr operation,
        TExecNodePtr node,
        const NProto::TJobSpec& spec) = 0;
};

struct IOperationController
{
    virtual ~IOperationController()
    { }


    virtual void Initialize() = 0;

    //virtual void Abort() = 0;

    //virtual void Complete() = 0;


    virtual void OnJobRunning(TJobPtr job) = 0;

    virtual void OnJobCompleted(TJobPtr job) = 0;

    virtual void OnJobFailed(TJobPtr job) = 0;


    virtual void ScheduleJobs(
        TExecNodePtr node,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
