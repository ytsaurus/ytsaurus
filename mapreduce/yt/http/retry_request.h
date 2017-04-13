#pragma once

#include <mapreduce/yt/interface/fwd.h>

#include <util/datetime/base.h>
#include <util/generic/stroka.h>

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
    virtual Stroka GetAttemptDescription() const = 0;
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
    virtual Stroka GetAttemptDescription() const override;

private:
    const ui32 AttemptLimit_;
    ui32 Attempt_ = 0;
};

////////////////////////////////////////////////////////////////////

struct TResponseInfo
{
    Stroka RequestId;
    Stroka Response;
};

////////////////////////////////////////////////////////////////////

struct TRequestConfig
{
    TDuration SocketTimeout = TDuration::Zero();
};

////////////////////////////////////////////////////////////////////

TResponseInfo RetryRequest(
    const TAuth& auth,
    THttpHeader& header,
    TStringBuf body,
    IRetryPolicy& retryPolicy,
    const TRequestConfig& config = TRequestConfig());

////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
