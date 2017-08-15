#include "raw_requests.h"

#include "batch_request_impl.h"

#include <mapreduce/yt/common/finally_guard.h>
#include <mapreduce/yt/http/retry_request.h>
#include <mapreduce/yt/node/node_io.h>

namespace NYT {
namespace NDetail {

void ExecuteBatch(
    const TAuth& auth,
    TRawBatchRequest& batchRequest,
    const TExecuteBatchOptions& options,
    IRetryPolicy& retryPolicy)
{
    if (batchRequest.IsExecuted()) {
        ythrow yexception() << "Cannot execute batch request since it is alredy executed";
    }
    NDetail::TFinallyGuard g([&] {
        batchRequest.MarkExecuted();
    });

    const auto concurrency = options.Concurrency_.GetOrElse(50);
    const auto batchPartMaxSize = options.BatchPartMaxSize_.GetOrElse(concurrency * 5);

    while (batchRequest.BatchSize()) {
        NDetail::TRawBatchRequest retryBatch;

        while (batchRequest.BatchSize()) {
            auto parameters = TNode::CreateMap();
            TInstant nextTry;
            batchRequest.FillParameterList(batchPartMaxSize, &parameters["requests"], &nextTry);
            if (nextTry) {
                SleepUntil(nextTry);
            }
            parameters["concurrency"] = concurrency;
            auto body = NodeToYsonString(parameters);
            THttpHeader header("POST", "execute_batch");
            header.AddMutationId();
            NDetail::TResponseInfo result;
            try {
                result = RetryRequest(auth, header, body, retryPolicy);
            } catch (const yexception& e) {
                batchRequest.SetErrorResult(std::current_exception());
                retryBatch.SetErrorResult(std::current_exception());
                throw;
            }
            batchRequest.ParseResponse(std::move(result), retryPolicy, &retryBatch);
        }

        batchRequest = std::move(retryBatch);
    }
}

} // namespace NDetail
} // namespace NYT
