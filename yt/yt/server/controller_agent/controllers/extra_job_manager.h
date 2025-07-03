#pragma once

#include "private.h"

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

namespace NYT::NControllerAgent::NControllers {

/////////////////////////////////////////////////////////////////////////////////

class IExtraJobManager
{
public:
    IExtraJobManager() = default;

    virtual i64 GetPendingCandidatesDataWeight() const = 0;

    virtual int GetPendingJobCount() const = 0;

    virtual int GetTotalJobCount() const = 0;

    //! Returns true if the cookie should be returned back to the pool.
    virtual bool OnJobAborted(const TJobletPtr& joblet, EAbortReason reason) = 0;
    virtual bool OnJobFailed(const TJobletPtr& joblet) = 0;

    virtual void OnJobLost(NChunkPools::IChunkPoolOutput::TCookie cookie) = 0;

    virtual void OnJobScheduled(const TJobletPtr& joblet) = 0;

    virtual void OnOperationRevived();

    //! Returns true if the cookie processing is finished.
    virtual bool OnJobCompleted(const TJobletPtr& joblet) = 0;

    virtual std::optional<EAbortReason> ShouldAbortCompletingJob(const TJobletPtr& joblet) = 0;

    virtual bool IsFinished() const = 0;

    virtual TProgressCounterPtr GetProgressCounter() const = 0;
};

/////////////////////////////////////////////////////////////////////////////////

} // NYT::NControllerAgent::NControllers
