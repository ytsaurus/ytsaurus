#include "retry_heavy_write_request.h"

#include "transaction.h"

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/retry_lib.h>
#include <mapreduce/yt/common/wait_proxy.h>

#include <mapreduce/yt/interface/logging/log.h>

#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/retry_request.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void RetryHeavyWriteRequest(
    const TAuth& auth,
    const TTransactionId& parentId,
    THttpHeader& header,
    std::function<THolder<IInputStream>()> streamMaker)
{
    int retryCount = TConfig::Get()->RetryCount;
    header.SetToken(auth.Token);

    for (int attempt = 0; attempt < retryCount; ++attempt) {
        TPingableTransaction attemptTx(auth, parentId);

        auto input = streamMaker();
        TString requestId;

        try {
            auto proxyName = GetProxyForHeavyRequest(auth);
            THttpRequest request;
            requestId = request.GetRequestId();

            header.AddTransactionId(attemptTx.GetId(), /* overwrite = */ true);
            header.SetRequestCompression(ToString(TConfig::Get()->ContentEncoding));

            request.Connect(proxyName);
            try {
                IOutputStream* output = request.StartRequest(header);
                TransferData(input.Get(), output);
                request.FinishRequest();
            } catch (yexception&) {
                // try to read error in response
            }
            request.GetResponse();

        } catch (TErrorResponse& e) {
            LOG_ERROR("RSP %s - attempt %d failed",
                requestId.data(),
                attempt);

            if (!IsRetriable(e) || attempt + 1 == retryCount) {
                throw;
            }
            NDetail::TWaitProxy::Get()->Sleep(GetRetryInterval(e));
            continue;

        } catch (yexception& e) {
            LOG_ERROR("RSP %s - %s - attempt %d failed",
                requestId.data(),
                e.what(),
                attempt);

            if (attempt + 1 == retryCount) {
                throw;
            }
            NDetail::TWaitProxy::Get()->Sleep(TConfig::Get()->RetryInterval);
            continue;
        }

        attemptTx.Commit();
        return;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
