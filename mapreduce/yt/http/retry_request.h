#pragma once

#include <mapreduce/yt/interface/fwd.h>
#include <mapreduce/yt/interface/retry_policy.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NYT {

struct TAuth;
class THttpHeader;
class TErrorResponse;

namespace NDetail {

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
    bool IsHeavy = false;
};

////////////////////////////////////////////////////////////////////

// Retry request with given `header' and `body' using `retryPolicy'.
// If `retryPolicy == nullptr' use default, currently `TAttemptLimitedRetryPolicy(TConfig::Get()->RetryCount)`.
TResponseInfo RetryRequestWithPolicy(
    const TAuth& auth,
    THttpHeader& header,
    TStringBuf body,
    IRequestRetryPolicy* retryPolicy = nullptr,
    const TRequestConfig& config = TRequestConfig());

////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
