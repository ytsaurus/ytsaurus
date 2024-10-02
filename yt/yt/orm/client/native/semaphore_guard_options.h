#pragma once

#include <yt/yt/core/misc/error.h>

#include <library/cpp/retry/retry.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

class TSemaphoreGuardOptions
{
public:
    using TRetryPolicyPtr = typename IRetryPolicy<const TErrorException&>::TPtr;

    struct TAcquireOptions
    {
        TRetryOptions RetryOptions;

        std::optional<TString> LeaseUuid;
        TDuration LeaseDuration;
        ui64 LeaseBudget;
    };

    // The ping scheme is as follows:
    // Ping master each period |Period|, with |RetryOptions.RetryCount| attempts to account for errors.
    // Each attempt times out after |Client.Options.Timeout|.
    // In case of a failure, new attempt starts after interval
    // chosen at random from range [AttemptSplay / 2, AttemptSplay).
    struct TPingOptions
    {
        TRetryOptions RetryOptions;

        TDuration Period;
    };

    struct TReleaseOptions
    {
        TRetryOptions RetryOptions;
    };

    TSemaphoreGuardOptions(
        TAcquireOptions acquireOptions,
        std::optional<TPingOptions> pingOptions,
        TReleaseOptions releaseOptions,
        IInvokerPtr invoker = nullptr);

    // Default options, with pinging the lease.
    static TSemaphoreGuardOptions GetDefault();

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TAcquireOptions, AcquireOptions);
    // If std::nullopt is set, semaphore guard does not ping the lease.
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(std::optional<TPingOptions>, PingOptions);
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TReleaseOptions, ReleaseOptions);

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(IInvokerPtr, Invoker);

private:
    void ValidateOptions() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
