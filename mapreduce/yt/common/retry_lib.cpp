#include "retry_lib.h"

#include "config.h"
#include <mapreduce/yt/interface/error_codes.h>

#include <util/string/builder.h>
#include <util/generic/set.h>

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

class TTimeLimitedRetryPolicy
    : public IRequestRetryPolicy
{
public:
    TTimeLimitedRetryPolicy(IRequestRetryPolicyPtr retryPolicy, TDuration timeout)
        : RetryPolicy_(retryPolicy)
        , Deadline_(TInstant::Now() + timeout)
        , Timeout_(timeout)
    { }
    void NotifyNewAttempt() override
    {
        if (TInstant::Now() >= Deadline_) {
            ythrow TRequestRetriesTimeout() << "retry timeout exceeded (timeout: " << Timeout_ << ")";
        }
        RetryPolicy_->NotifyNewAttempt();
    }

    TMaybe<TDuration> OnGenericError(const yexception& e) override
    {
        return RetryPolicy_->OnGenericError(e);
    }

    TMaybe<TDuration> OnRetriableError(const TErrorResponse& e) override
    {
        return RetryPolicy_->OnRetriableError(e);
    }

    void OnIgnoredError(const TErrorResponse& e) override
    {
        return RetryPolicy_->OnIgnoredError(e);
    }

    TString GetAttemptDescription() const override
    {
        return RetryPolicy_->GetAttemptDescription();
    }

private:
    const IRequestRetryPolicyPtr RetryPolicy_;
    const TInstant Deadline_;
    const TDuration Timeout_;
};

////////////////////////////////////////////////////////////////////////////////

class TDefaultClientRetryPolicy
    : public IClientRetryPolicy
{
public:
    explicit TDefaultClientRetryPolicy(IRetryConfigProviderPtr retryConfigProvider)
        : RetryConfigProvider_(std::move(retryConfigProvider))
    { }

    IRequestRetryPolicyPtr CreatePolicyForGenericRequest() override
    {
        return Wrap(CreateDefaultRequestRetryPolicy());
    }

    IRequestRetryPolicyPtr CreatePolicyForStartOperationRequest() override
    {
        return Wrap(MakeIntrusive<TAttemptLimitedRetryPolicy>(static_cast<ui32>(TConfig::Get()->StartOperationRetryCount)));
    }

    IRequestRetryPolicyPtr Wrap(IRequestRetryPolicyPtr basePolicy)
    {
        auto config = RetryConfigProvider_->CreateRetryConfig();
        if (config.RetriesTimeLimit < TDuration::Max()) {
            return ::MakeIntrusive<TTimeLimitedRetryPolicy>(std::move(basePolicy), config.RetriesTimeLimit);
        }
        return basePolicy;
    }

private:
    IRetryConfigProviderPtr RetryConfigProvider_;
};

class TDefaultRetryConfigProvider
    : public IRetryConfigProvider
{
public:
    TRetryConfig CreateRetryConfig() override
    {
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

IRequestRetryPolicyPtr CreateDefaultRequestRetryPolicy()
{
    return MakeIntrusive<TAttemptLimitedRetryPolicy>(static_cast<ui32>(TConfig::Get()->RetryCount));
}

IClientRetryPolicyPtr CreateDefaultClientRetryPolicy(IRetryConfigProviderPtr retryConfigProvider)
{
    return MakeIntrusive<TDefaultClientRetryPolicy>(std::move(retryConfigProvider));
}
IRetryConfigProviderPtr CreateDefaultRetryConfigProvider()
{
    return MakeIntrusive<TDefaultRetryConfigProvider>();
}

////////////////////////////////////////////////////////////////////////////////

static std::pair<bool,TDuration> GetRetryInfo(const TErrorResponse& errorResponse)
{
    bool retriable = true;
    TDuration retryInterval = TConfig::Get()->RetryInterval;

    int code = errorResponse.GetError().GetInnerCode();
    auto allCodes = errorResponse.GetError().GetAllErrorCodes();
    int httpCode = errorResponse.GetHttpCode();
    using namespace NClusterErrorCodes;
    if (httpCode / 100 == 4) {
        if (httpCode == 429
            || allCodes.count(NSecurityClient::RequestQueueSizeLimitExceeded)
            || allCodes.count(NRpc::RequestQueueSizeLimitExceeded))
        {
            // request rate limit exceeded
            retryInterval = TConfig::Get()->RateLimitExceededRetryInterval;
        } else if (errorResponse.IsConcurrentOperationsLimitReached()) {
            // limit for the number of concurrent operations exceeded
            retryInterval = TConfig::Get()->StartOperationRetryInterval;
        } else if (code / 100 == 7) {
            // chunk client errors
            retryInterval = TConfig::Get()->ChunkErrorsRetryInterval;
        } else if (
            allCodes.count(NRpc::TransportError)
            || allCodes.count(NRpc::Unavailable))
        {
            retriable = true;
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

bool IsRetriable(const yexception& ex)
{
    if (dynamic_cast<const TRequestRetriesTimeout*>(&ex)) {
        return false;
    }
    return true;
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
