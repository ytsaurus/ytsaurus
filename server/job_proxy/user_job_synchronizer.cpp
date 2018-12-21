#include "user_job_synchronizer.h"

#include <yt/core/concurrency/scheduler.h>

namespace NYT::NJobProxy {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TUserJobSynchronizer::NotifyJobSatellitePrepared(const TErrorOr<i64>& rssOrError)
{
    JobSatellitePreparedPromise_.TrySet(rssOrError);
}

void TUserJobSynchronizer::NotifyExecutorPrepared()
{
    ExecutorPreparedPromise_.TrySet(TError());
}

void TUserJobSynchronizer::NotifyUserJobFinished(const TError& error)
{
    UserJobFinishedPromise_.TrySet(error);
}

void TUserJobSynchronizer::Wait()
{
    JobSatelliteRssUsage_ = WaitFor(JobSatellitePreparedPromise_.ToFuture())
        .ValueOrThrow();
    WaitFor(ExecutorPreparedPromise_.ToFuture())
        .ThrowOnError();
}

i64 TUserJobSynchronizer::GetJobSatelliteRssUsage() const
{
    return JobSatelliteRssUsage_;
}

TError TUserJobSynchronizer::GetUserProcessStatus() const
{
    if (!UserJobFinishedPromise_.IsSet()) {
        THROW_ERROR_EXCEPTION("Satellite did not finish successfully");
    }
    return UserJobFinishedPromise_.Get();
}

void TUserJobSynchronizer::CancelWait()
{
    JobSatellitePreparedPromise_.TrySet(TErrorOr<i64>(0));
    ExecutorPreparedPromise_.TrySet(TError());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
