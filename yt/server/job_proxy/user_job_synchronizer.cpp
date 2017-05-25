#include "user_job_synchronizer.h"

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NJobProxy {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TUserJobSynchronizer::NotifyJobSatellitePrepared()
{
    JobSatellitePreparedPromise_.Set(TError());
}

void TUserJobSynchronizer::NotifyExecutorPrepared()
{
   ExecutorPreparedPromise_.Set(TError());
}

void TUserJobSynchronizer::NotifyUserJobFinished(const TError &error)
{
    UserJobFinishedPromise_.Set(error);
}

void TUserJobSynchronizer::Wait()
{
    auto jobSatellitePreparedFuture = JobSatellitePreparedPromise_.ToFuture();
    auto executorPrepatedFuture = ExecutorPreparedPromise_.ToFuture();
    WaitFor(CombineAll(std::vector<TFuture<void>>{
        jobSatellitePreparedFuture,
        executorPrepatedFuture}
        ));
}

TError TUserJobSynchronizer::GetUserProcessStatus() const
{
    if (!UserJobFinishedPromise_.IsSet()) {
        THROW_ERROR_EXCEPTION("Satellite did not finish succefully");
    }
    return UserJobFinishedPromise_.Get();
}

void TUserJobSynchronizer::CancelWait()
{
    JobSatellitePreparedPromise_.TrySet(TError());
    ExecutorPreparedPromise_.TrySet(TError());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
