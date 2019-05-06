#include "retry_lib.h"

#include "config.h"

#include <util/string/builder.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TAttemptLimitedRetryPolicy::TAttemptLimitedRetryPolicy(ui32 attemptLimit)
    : AttemptLimit_(attemptLimit)
{ }

void TAttemptLimitedRetryPolicy::NotifyNewAttempt()
{
    ++Attempt_;
}

TMaybe<TDuration> TAttemptLimitedRetryPolicy::OnGenericError(const yexception& e)
{
    if (IsAttemptLimitExceeded()) {
        return Nothing();
    }
    return GetBackoffDuration(e);
}

TMaybe<TDuration> TAttemptLimitedRetryPolicy::OnRetriableError(const TErrorResponse& e)
{
    if (IsAttemptLimitExceeded()) {
        return Nothing();
    }
    return GetBackoffDuration(e);
}

void TAttemptLimitedRetryPolicy::OnIgnoredError(const TErrorResponse& /*e*/)
{
    --Attempt_;
}

TString TAttemptLimitedRetryPolicy::GetAttemptDescription() const
{
    return TStringBuilder() << "attempt " << Attempt_ << " of " << AttemptLimit_;
}

bool TAttemptLimitedRetryPolicy::IsAttemptLimitExceeded() const
{
    return Attempt_ >= AttemptLimit_;
}

////////////////////////////////////////////////////////////////////////////////

class TDefaultClientRetryPolicy
    : public IClientRetryPolicy
{
public:
    IRequestRetryPolicyPtr CreatePolicyForGenericRequest() override
    {
        return CreateDefaultRequestRetryPolicy();
    }
};

////////////////////////////////////////////////////////////////////////////////

IRequestRetryPolicyPtr CreateDefaultRequestRetryPolicy()
{
    return MakeIntrusive<TAttemptLimitedRetryPolicy>(static_cast<ui32>(TConfig::Get()->RetryCount));
}

IClientRetryPolicyPtr CreateDefaultClientRetryPolicy()
{
    return MakeIntrusive<TDefaultClientRetryPolicy>();
}

////////////////////////////////////////////////////////////////////////////////

static std::pair<bool,TDuration> GetRetryInfo(const TErrorResponse& errorResponse)
{
    bool retriable = true;
    TDuration retryInterval = TConfig::Get()->RetryInterval;

    int code = errorResponse.GetError().GetInnerCode();
    int httpCode = errorResponse.GetHttpCode();
    if (httpCode / 100 == 4) {
        if (httpCode == 429 || code == 904 || code == 108) {
            // request rate limit exceeded
            retryInterval = TConfig::Get()->RateLimitExceededRetryInterval;
        } else if (errorResponse.IsConcurrentOperationsLimitReached()) {
            // limit for the number of concurrent operations exceeded
            retryInterval = TConfig::Get()->StartOperationRetryInterval;
        } else if (code / 100 == 7) {
            // chunk client errors
            retryInterval = TConfig::Get()->ChunkErrorsRetryInterval;
        } else {
            retriable = false;
        }
    }
    return std::make_pair(retriable, retryInterval);
}

TDuration GetBackoffDuration(const TErrorResponse& errorResponse)
{
    return GetRetryInfo(errorResponse).second;
}

bool IsRetriable(const TErrorResponse& errorResponse)
{
    return GetRetryInfo(errorResponse).first;
}

TDuration GetBackoffDuration(const yexception& /*error*/)
{
    return GetBackoffDuration();
}

TDuration GetBackoffDuration()
{
    return TConfig::Get()->RetryInterval;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
