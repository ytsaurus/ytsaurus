#include "raw_requests.h"

#include "raw_batch_request.h"
#include "rpc_parameters_serialization.h"

#include <mapreduce/yt/common/helpers.h>
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
        ythrow yexception() << "Cannot execute batch request since it is already executed";
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
                result = RetryRequestWithPolicy(auth, header, body, retryPolicy);
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

TNode Get(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options)
{
    THttpHeader header("GET", "get");
    header.MergeParameters(NDetail::SerializeParamsForGet(transactionId, path, options));
    return NodeFromYsonString(RetryRequest(auth, header));
}

void Set(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options)
{
    THttpHeader header("PUT", "set");
    header.AddMutationId();
    header.MergeParameters(NDetail::SerializeParamsForSet(transactionId, path, options));
    RetryRequest(auth, header, NodeToYsonString(value));
}

bool Exists(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path)
{
    THttpHeader header("GET", "exists");
    header.MergeParameters(NDetail::SerializeParamsForExists(transactionId, path));
    return ParseBoolFromResponse(RetryRequest(auth, header));
}

TNodeId Create(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const ENodeType& type,
    const TCreateOptions& options)
{
    THttpHeader header("POST", "create");
    header.AddMutationId();
    header.MergeParameters(NDetail::SerializeParamsForCreate(transactionId, path, type, options));
    return ParseGuidFromResponse(RetryRequest(auth, header));
}

void Remove(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TRemoveOptions& options)
{
    THttpHeader header("POST", "remove");
    header.AddMutationId();
    header.MergeParameters(NDetail::SerializeParamsForRemove(transactionId, path, options));
    RetryRequest(auth, header);
}

TNode::TListType List(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TListOptions& options)
{
    THttpHeader header("GET", "list");

    TYPath updatedPath = AddPathPrefix(path);
    // Translate "//" to "/"
    // Translate "//some/constom/prefix/from/config/" to "//some/constom/prefix/from/config"
    if (path.empty() && updatedPath.EndsWith('/')) {
        updatedPath.pop_back();
    }
    header.MergeParameters(NDetail::SerializeParamsForList(transactionId, updatedPath, options));
    return NodeFromYsonString(RetryRequest(auth, header)).AsList();
}

TNodeId Link(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    THttpHeader header("POST", "link");
    header.AddMutationId();
    header.MergeParameters(NDetail::SerializeParamsForLink(transactionId, targetPath, linkPath, options));
    return ParseGuidFromResponse(RetryRequest(auth, header));
}

TLockId Lock(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    THttpHeader header("POST", "lock");
    header.AddMutationId();
    header.MergeParameters(NDetail::SerializeParamsForLock(transactionId, path, mode, options));
    return ParseGuidFromResponse(RetryRequest(auth, header));
}

} // namespace NDetail
} // namespace NYT
