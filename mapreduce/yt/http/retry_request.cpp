#include "retry_request.h"

#include "requests.h"

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/wait_proxy.h>
#include <mapreduce/yt/common/retry_lib.h>

#include <mapreduce/yt/interface/logging/log.h>

#include <mapreduce/yt/node/node_io.h>

#include <util/string/builder.h>

namespace NYT {
namespace NDetail {

///////////////////////////////////////////////////////////////////////////////

TResponseInfo RetryRequestWithPolicy(
    const TAuth& auth,
    THttpHeader& header,
    TStringBuf body,
    IRequestRetryPolicy* retryPolicy,
    const TRequestConfig& config)
{
    header.SetToken(auth.Token);

    bool useMutationId = header.HasMutationId();
    bool retryWithSameMutationId = false;

    IRequestRetryPolicyPtr defaultRetryPolicy = nullptr;
    if (!retryPolicy) {
        defaultRetryPolicy = CreateDefaultRetryPolicy();
        retryPolicy = defaultRetryPolicy.Get();
    }

    while (true) {
        retryPolicy->NotifyNewAttempt();
        THttpHeader currentHeader = header;
        TString response;

        TString requestId = "<unknown>";
        try {
            TString hostName;
            if (config.IsHeavy) {
                hostName = GetProxyForHeavyRequest(auth);
            } else {
                hostName = auth.ServerName;
            }
            THttpRequest request(hostName);
            TString requestId = request.GetRequestId();

            if (useMutationId) {
                if (retryWithSameMutationId) {
                    header.AddParameter("retry", true, /* overwrite = */ true);
                } else {
                    header.RemoveParameter("retry");
                    header.AddMutationId();
                }
            }

            request.Connect(config.SocketTimeout);
            request.SmallRequest(header, body);

            TResponseInfo result;
            result.RequestId = requestId;
            result.Response = request.GetResponse();
            return result;
        } catch (const TErrorResponse& e) {
            LogRequestError(requestId, header, e.GetError().GetMessage(), retryPolicy->GetAttemptDescription());
            retryWithSameMutationId = false;

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
            LogRequestError(requestId, header, e.what(), retryPolicy->GetAttemptDescription());
            retryWithSameMutationId = true;

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
