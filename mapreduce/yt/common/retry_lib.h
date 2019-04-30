#pragma once

#include <mapreduce/yt/interface/retry_policy.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TAttemptLimitedRetryPolicy
    : public IRequestRetryPolicy
{
public:
    TAttemptLimitedRetryPolicy(ui32 attemptLimit);

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

IRequestRetryPolicyPtr CreateDefaultRetryPolicy();

////////////////////////////////////////////////////////////////////////////////

bool IsRetriable(const TErrorResponse& errorResponse);
TDuration GetBackoffDuration(const TErrorResponse& errorResponse);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT