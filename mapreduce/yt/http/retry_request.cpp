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
    if (!e.IsRetriable()) {
        return Nothing();
    }
    return e.GetRetryInterval();
}

Stroka TAttemptLimitedRetryPolicy::GetAttemptDescription() const
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
        Stroka response;

        THttpRequest request(auth.ServerName);
        Stroka requestId = request.GetRequestId();
        try {
            Stroka hostName = auth.ServerName;

            if (useMutationId) {
                if (retryWithSameMutationId) {
                    header.AddParam("retry", "true");
                } else {
                    header.RemoveParam("retry");
                    header.AddMutationId();
                }
            }

            request.Connect(config.SocketTimeout);

            TOutputStream* output = request.StartRequest(header);
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

////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
