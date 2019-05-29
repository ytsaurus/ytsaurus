#pragma once

#include <mapreduce/yt/interface/retry_policy.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TAttemptLimitedRetryPolicy
    : public IRequestRetryPolicy
{
public:
    explicit TAttemptLimitedRetryPolicy(ui32 attemptLimit);

    void NotifyNewAttempt() override;

    TMaybe<TDuration> OnGenericError(const yexception& e) override;
    TMaybe<TDuration> OnRetriableError(const TErrorResponse& e) override;
    void OnIgnoredError(const TErrorResponse& e) override;
    TString GetAttemptDescription() const override;

    bool IsAttemptLimitExceeded() const;

private:
    const ui32 AttemptLimit_;
    ui32 Attempt_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRequestRetryPolicyPtr CreateDefaultRequestRetryPolicy();
IClientRetryPolicyPtr CreateDefaultClientRetryPolicy(IRetryConfigProviderPtr retryConfigProvider);
IRetryConfigProviderPtr CreateDefaultRetryConfigProvider();

////////////////////////////////////////////////////////////////////////////////

// Check if error returned by YT can be retried
bool IsRetriable(const TErrorResponse& errorResponse);
bool IsRetriable(const yexception& ex);

// Get backoff duration for errors returned by YT.
TDuration GetBackoffDuration(const TErrorResponse& errorResponse);

// Get backoff duration for errors that are not TErrorResponse.
TDuration GetBackoffDuration(const yexception& error);
TDuration GetBackoffDuration();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
