#include "semaphore_guard_options.h"

#include <yt/yt/orm/client/objects/public.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

TSemaphoreGuardOptions::TSemaphoreGuardOptions(
    TAcquireOptions acquireOptions,
    std::optional<TPingOptions> pingOptions,
    TReleaseOptions releaseOptions,
    IInvokerPtr invoker)
    : AcquireOptions_(std::move(acquireOptions))
    , PingOptions_(std::move(pingOptions))
    , ReleaseOptions_(std::move(releaseOptions))
    , Invoker_(std::move(invoker))
{
    ValidateOptions();
};

TSemaphoreGuardOptions TSemaphoreGuardOptions::GetDefault()
{
    auto retryOptions = TRetryOptions()
        .WithCount(3)
        .WithSleep(TDuration::MilliSeconds(500))
        .WithRandomDelta(TDuration::MilliSeconds(500));

    TSemaphoreGuardOptions::TAcquireOptions acquireOptions;
    acquireOptions.RetryOptions = retryOptions;
    acquireOptions.LeaseDuration = TDuration::Minutes(2);
    acquireOptions.LeaseBudget = 1;

    TSemaphoreGuardOptions::TPingOptions pingOptions;
    pingOptions.RetryOptions = retryOptions;
    pingOptions.Period = TDuration::Seconds(1);

    TSemaphoreGuardOptions::TReleaseOptions releaseOptions;
    releaseOptions.RetryOptions = retryOptions;

    acquireOptions.RetryOptions.RetryCount = 5;
    releaseOptions.RetryOptions.RetryCount = 5;

    return TSemaphoreGuardOptions(
        acquireOptions,
        pingOptions,
        releaseOptions);
}

void TSemaphoreGuardOptions::ValidateOptions() const
{
    auto checkAttemptCount = [] (int attempts, TStringBuf source) {
        THROW_ERROR_EXCEPTION_UNLESS(attempts > 0,
            "Invalid semaphore guard options: expected attempt count > 0 for %v",
            source);
    };
    checkAttemptCount(AcquireOptions_.RetryOptions.RetryCount, "acquire");
    checkAttemptCount(ReleaseOptions_.RetryOptions.RetryCount, "release");
    if (PingOptions_) {
        checkAttemptCount(PingOptions_->RetryOptions.RetryCount, "ping");

        auto pingPeriod = PingOptions_->Period;
        const auto& retries = PingOptions_->RetryOptions;
        auto totalDuration = pingPeriod + (retries.SleepDuration + retries.SleepRandomDelta) * retries.RetryCount;

        THROW_ERROR_EXCEPTION_IF(totalDuration >= AcquireOptions_.LeaseDuration,
            "Invalid semaphore guard options: "
            "worst case time for refreshing the lease must be less than lease duration");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
