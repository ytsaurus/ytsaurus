#include "retry_heavy_write_request.h"

#include "transaction.h"
#include "transaction_pinger.h"

#include <mapreduce/yt/common/retry_lib.h>
#include <mapreduce/yt/common/wait_proxy.h>

#include <mapreduce/yt/interface/config.h>
#include <mapreduce/yt/interface/tvm.h>

#include <mapreduce/yt/interface/logging/yt_log.h>

#include <mapreduce/yt/http/helpers.h>
#include <mapreduce/yt/http/http_client.h>
#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/retry_request.h>

namespace NYT {

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

void RetryHeavyWriteRequest(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const ITransactionPingerPtr& transactionPinger,
    const TClientContext& context,
    const TTransactionId& parentId,
    THttpHeader& header,
    std::function<THolder<IInputStream>()> streamMaker)
{
    int retryCount = context.Config->RetryCount;
    header.SetToken(context.Token);
    if (context.ServiceTicketAuth) {
        header.SetServiceTicket(context.ServiceTicketAuth->Ptr->IssueServiceTicket());
    }

    for (int attempt = 0; attempt < retryCount; ++attempt) {
        TPingableTransaction attemptTx(clientRetryPolicy, context, parentId, transactionPinger->GetChildTxPinger(), TStartTransactionOptions());

        auto input = streamMaker();
        TString requestId;

        try {
            auto hostName = GetProxyForHeavyRequest(context);
            requestId = CreateGuidAsString();

            header.AddTransactionId(attemptTx.GetId(), /* overwrite = */ true);
            header.SetRequestCompression(ToString(context.Config->ContentEncoding));

            auto request = context.HttpClient->StartRequest(GetFullUrl(hostName, context, header), requestId, header);
            TransferData(input.Get(), request->GetStream());
            request->Finish()->GetResponse();
        } catch (TErrorResponse& e) {
            YT_LOG_ERROR("RSP %v - attempt %v failed",
                requestId,
                attempt);

            if (!IsRetriable(e) || attempt + 1 == retryCount) {
                throw;
            }
            NDetail::TWaitProxy::Get()->Sleep(GetBackoffDuration(e, context.Config));
            continue;

        } catch (std::exception& e) {
            YT_LOG_ERROR("RSP %v - %v - attempt %v failed",
                requestId,
                e.what(),
                attempt);

            if (attempt + 1 == retryCount) {
                throw;
            }
            NDetail::TWaitProxy::Get()->Sleep(GetBackoffDuration(e, context.Config));
            continue;
        }

        attemptTx.Commit();
        return;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
