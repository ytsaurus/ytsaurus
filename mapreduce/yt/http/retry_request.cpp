#include "retry_request.h"

#include "context.h"
#include "helpers.h"
#include "http_client.h"
#include "requests.h"

#include <mapreduce/yt/common/wait_proxy.h>
#include <mapreduce/yt/common/retry_lib.h>

#include <mapreduce/yt/interface/config.h>
#include <mapreduce/yt/interface/tvm.h>

#include <mapreduce/yt/interface/logging/yt_log.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT {
namespace NDetail {

///////////////////////////////////////////////////////////////////////////////

static TResponseInfo Request(
    const TClientContext& context,
    THttpHeader& header,
    TMaybe<TStringBuf> body,
    const TString& requestId,
    const TRequestConfig& config)
{
    TString hostName;
    if (config.IsHeavy) {
        hostName = GetProxyForHeavyRequest(context);
    } else {
        hostName = context.ServerName;
    }

    auto url = GetFullUrl(hostName, context, header);

    auto response = context.HttpClient->Request(url, requestId, config.HttpConfig, header, body);

    TResponseInfo result;
    result.RequestId = requestId;
    result.Response = response->GetResponse();
    result.HttpCode = response->GetStatusCode();
    return result;
}

TResponseInfo RequestWithoutRetry(
    const TClientContext& context,
    THttpHeader& header,
    TMaybe<TStringBuf> body,
    const TRequestConfig& config)
{
    header.SetToken(context.Token);
    if (context.ServiceTicketAuth) {
        header.SetServiceTicket(context.ServiceTicketAuth->Ptr->IssueServiceTicket());
    }

    if (header.HasMutationId()) {
        header.RemoveParameter("retry");
        header.AddMutationId();
    }
    auto requestId = CreateGuidAsString();
    return Request(context, header, body, requestId, config);
}


TResponseInfo RetryRequestWithPolicy(
    IRequestRetryPolicyPtr retryPolicy,
    const TClientContext& context,
    THttpHeader& header,
    TMaybe<TStringBuf> body,
    const TRequestConfig& config)
{
    header.SetToken(context.Token);
    if (context.ServiceTicketAuth) {
        header.SetServiceTicket(context.ServiceTicketAuth->Ptr->IssueServiceTicket());
    }

    bool useMutationId = header.HasMutationId();
    bool retryWithSameMutationId = false;

    if (!retryPolicy) {
        retryPolicy = CreateDefaultRequestRetryPolicy(context.Config);
    }

    while (true) {
        auto requestId = CreateGuidAsString();
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

            return Request(context, header, body, requestId, config);
        } catch (const TErrorResponse& e) {
            LogRequestError(requestId, header, e.GetError().GetMessage(), retryPolicy->GetAttemptDescription());
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
        } catch (const std::exception& e) {
            LogRequestError(requestId, header, e.what(), retryPolicy->GetAttemptDescription());
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
