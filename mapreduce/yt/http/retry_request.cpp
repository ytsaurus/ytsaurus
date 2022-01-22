#include "retry_request.h"

#include "requests.h"

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/wait_proxy.h>
#include <mapreduce/yt/common/retry_lib.h>

#include <mapreduce/yt/interface/logging/yt_log.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT {
namespace NDetail {

///////////////////////////////////////////////////////////////////////////////

static TResponseInfo Request(
    const TAuth &auth,
    THttpHeader &header,
    TMaybe<TStringBuf> body,
    THttpRequest& request,
    const TRequestConfig &config)
{
    TString hostName;
    if (config.IsHeavy) {
        hostName = GetProxyForHeavyRequest(auth);
    } else {
        hostName = auth.ServerName;
    }

    request.Connect(hostName, config.SocketTimeout);

    request.SmallRequest(header, body);

    TResponseInfo result;
    result.RequestId = request.GetRequestId();
    result.Response = request.GetResponse();
    result.HttpCode = request.GetHttpCode();
    return result;
}

TResponseInfo RequestWithoutRetry(
    const TAuth& auth,
    THttpHeader& header,
    TMaybe<TStringBuf> body,
    const TRequestConfig& config)
{
    header.SetToken(auth.Token);
    if (header.HasMutationId()) {
        header.RemoveParameter("retry");
        header.AddMutationId();
    }
    THttpRequest request;
    return Request(auth, header, body, request, config);
}


TResponseInfo RetryRequestWithPolicy(
    IRequestRetryPolicyPtr retryPolicy,
    const TAuth& auth,
    THttpHeader& header,
    TMaybe<TStringBuf> body,
    const TRequestConfig& config)
{
    header.SetToken(auth.Token);

    bool useMutationId = header.HasMutationId();
    bool retryWithSameMutationId = false;

    if (!retryPolicy) {
        retryPolicy = CreateDefaultRequestRetryPolicy();
    }

    while (true) {
        THttpRequest request;
        try {
            retryPolicy->NotifyNewAttempt();

            if (useMutationId) {
                if (retryWithSameMutationId) {
                    header.AddParameter("retry", true, /* overwrite = */ true);
                } else {
                    header.RemoveParameter("retry");
                    header.AddMutationId();
                }
            }

            return Request(auth, header, body, request, config);
        } catch (const TErrorResponse& e) {
            LogRequestError(request, header, e.GetError().GetMessage(), retryPolicy->GetAttemptDescription());
            retryWithSameMutationId = e.IsTransportError();

            if (!IsRetriable(e)) {
                throw;
            }

            auto maybeRetryTimeout = retryPolicy->OnRetriableError(e);
            if (maybeRetryTimeout) {
                TWaitProxy::Get()->Sleep(*maybeRetryTimeout);
            } else {
                throw;
            }
        } catch (const yexception& e) {
            LogRequestError(request, header, e.what(), retryPolicy->GetAttemptDescription());
            retryWithSameMutationId = true;

            if (!IsRetriable(e)) {
                throw;
            }

            auto maybeRetryTimeout = retryPolicy->OnGenericError(e);
            if (maybeRetryTimeout) {
                TWaitProxy::Get()->Sleep(*maybeRetryTimeout);
            } else {
                throw;
            }
        }
    }

    Y_FAIL("Retries must have either succeeded or thrown an exception");
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
