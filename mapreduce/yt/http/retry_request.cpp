#include "retry_request.h"

#include "requests.h"

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/log.h>

#include <util/stream/str.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////

TAttemptLimitedRetryPolicy::TAttemptLimitedRetryPolicy(ui32 attemptLimit)
    : AttemptLimit_(attemptLimit)
{ }

void TAttemptLimitedRetryPolicy::NotifyNewAttempt()
{
    ++Attempt_;
}

TMaybe<TDuration> TAttemptLimitedRetryPolicy::GetRetryInterval(const yexception& /*e*/) const
{
    if (Attempt_ > AttemptLimit_) {
        return Nothing();
    }
    return TConfig::Get()->RetryInterval;
}

TMaybe<TDuration> TAttemptLimitedRetryPolicy::GetRetryInterval(const TErrorResponse& e) const
{
    if (Attempt_ > AttemptLimit_) {
        return Nothing();
    }
    if (!IsRetriable(e)) {
        return Nothing();
    }
    return NYT::NDetail::GetRetryInterval(e);
}

TString TAttemptLimitedRetryPolicy::GetAttemptDescription() const
{
    TStringStream s;
    s << "attempt " << Attempt_ << " of " << AttemptLimit_;
    return s.Str();
}

////////////////////////////////////////////////////////////////////

TResponseInfo RetryRequest(
    const TAuth& auth,
    THttpHeader& header,
    TStringBuf body,
    IRetryPolicy& retryPolicy,
    const TRequestConfig& config)
{
    header.SetToken(auth.Token);

    bool useMutationId = header.HasMutationId();
    bool retryWithSameMutationId = false;

    while (true) {
        retryPolicy.NotifyNewAttempt();
        THttpHeader currentHeader = header;
        TString response;

        THttpRequest request(auth.ServerName);
        TString requestId = request.GetRequestId();
        try {
            TString hostName = auth.ServerName;

            if (useMutationId) {
                if (retryWithSameMutationId) {
                    header.AddParam("retry", "true");
                } else {
                    header.RemoveParam("retry");
                    header.AddMutationId();
                }
            }

            request.Connect(config.SocketTimeout);

            IOutputStream* output = request.StartRequest(header);
            output->Write(body);
            request.FinishRequest();

            TResponseInfo result;
            result.RequestId = requestId;
            result.Response = request.GetResponse();
            return result;
        } catch (const TErrorResponse& e) {
            LOG_ERROR("RSP %s - %s - failed (%s)",
                ~e.GetError().GetMessage(),
                ~requestId,
                ~retryPolicy.GetAttemptDescription());
            retryWithSameMutationId = false;

            auto maybeRetryTimeout = retryPolicy.GetRetryInterval(e);
            if (maybeRetryTimeout) {
                Sleep(*maybeRetryTimeout);
            } else {
                throw;
            }
        } catch (const yexception& e) {
            LOG_ERROR("RSP %s - %s - failed (%s)",
                ~requestId,
                e.what(),
                ~retryPolicy.GetAttemptDescription());
            retryWithSameMutationId = true;

            auto maybeRetryTimeout = retryPolicy.GetRetryInterval(e);
            if (maybeRetryTimeout) {
                Sleep(*maybeRetryTimeout);
            } else {
                throw;
            }
        }
    }

    Y_UNREACHABLE();
}

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

TDuration GetRetryInterval(const TErrorResponse& errorResponse)
{
    return GetRetryInfo(errorResponse).second;
}

bool IsRetriable(const TErrorResponse& errorResponse)
{
    return GetRetryInfo(errorResponse).first;
}

////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
