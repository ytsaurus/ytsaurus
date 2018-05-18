#pragma once

#include <mapreduce/yt/interface/fwd.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NYT {

struct TAuth;
class THttpHeader;
class TErrorResponse;

namespace NDetail {

////////////////////////////////////////////////////////////////////

struct IRetryPolicy
{
    virtual ~IRetryPolicy() = default;

    virtual void NotifyNewAttempt() = 0;

    // Return Nothing() if retries must not be continued.
    virtual TMaybe<TDuration> GetRetryInterval(const yexception& e) const = 0;
    virtual TMaybe<TDuration> GetRetryInterval(const TErrorResponse& e) const = 0;
    virtual TString GetAttemptDescription() const = 0;
};

////////////////////////////////////////////////////////////////////

class TAttemptLimitedRetryPolicy
    : public IRetryPolicy
{
public:
    TAttemptLimitedRetryPolicy(ui32 attemptLimit);

    virtual void NotifyNewAttempt() override;

    virtual TMaybe<TDuration> GetRetryInterval(const yexception& e) const override;
    virtual TMaybe<TDuration> GetRetryInterval(const TErrorResponse& e) const override;
    virtual TString GetAttemptDescription() const override;

    bool IsAttemptLimitExceeded() const;

private:
    const ui32 AttemptLimit_;
    ui32 Attempt_ = 0;
};

////////////////////////////////////////////////////////////////////

struct TResponseInfo
{
    TString RequestId;
    TString Response;
};

////////////////////////////////////////////////////////////////////

struct TRequestConfig
{
    TDuration SocketTimeout = TDuration::Zero();
};

////////////////////////////////////////////////////////////////////

// Retry request with given `header' and `body' using `retryPolicy'.
// If `retryPolicy == nullptr' use default, currently TAttemptLimitedRetryPolicy(TConfig::Get()->RetryCount)
TResponseInfo RetryRequestWithPolicy(
    const TAuth& auth,
    THttpHeader& header,
    TStringBuf body,
    IRetryPolicy* retryPolicy = nullptr,
    const TRequestConfig& config = TRequestConfig());

bool IsRetriable(const TErrorResponse& errorResponse);
TDuration GetRetryInterval(const TErrorResponse& errorResponse);

////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
