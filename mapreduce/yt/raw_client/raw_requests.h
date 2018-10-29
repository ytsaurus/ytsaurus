#pragma once

#include <mapreduce/yt/interface/fwd.h>
#include <mapreduce/yt/interface/client_method_options.h>
#include <mapreduce/yt/interface/operation.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class IRetryPolicy;
struct TAuth;
struct TExecuteBatchOptions;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

struct IRetryPolicy;
class TRawBatchRequest;

////////////////////////////////////////////////////////////////////////////////

TOperationAttributes ParseOperationAttributes(const TNode& node);

//
// marks `batchRequest' as executed
void ExecuteBatch(
    const TAuth& auth,
    TRawBatchRequest& batchRequest,
    const TExecuteBatchOptions& options = TExecuteBatchOptions(),
    IRetryPolicy* retryPolicy = nullptr);

TNode Get(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options = TGetOptions(),
    IRetryPolicy* retryPolicy = nullptr);

void Set(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options = TSetOptions(),
    IRetryPolicy* retryPolicy = nullptr);

bool Exists(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    IRetryPolicy* retryPolicy = nullptr);

TNodeId Create(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const ENodeType& type,
    const TCreateOptions& options = TCreateOptions(),
    IRetryPolicy* retryPolicy = nullptr);

void Remove(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TRemoveOptions& options = TRemoveOptions(),
    IRetryPolicy* retryPolicy = nullptr);

TNode::TListType List(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TListOptions& options,
    IRetryPolicy* retryPolicy = nullptr);

TNodeId Link(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options = TLinkOptions(),
    IRetryPolicy* retryPolicy = nullptr);

TLockId Lock(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options = TLockOptions(),
    IRetryPolicy* retryPolicy = nullptr);

void PingTx(
    const TAuth& auth,
    const TTransactionId& transactionId,
    IRetryPolicy* retryPolicy = nullptr);

TOperationAttributes GetOperation(
    const TAuth& auth,
    const TOperationId& operationId,
    const TGetOperationOptions& options = TGetOperationOptions(),
    IRetryPolicy* retryPolicy = nullptr);

TListOperationsResult ListOperations(
    const TAuth& auth,
    const TListOperationsOptions& options = TListOperationsOptions(),
    IRetryPolicy* retryPolicy = nullptr);

void UpdateOperationParameters(
    const TAuth& auth,
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options,
    IRetryPolicy* retryPolicy = nullptr);

TNode ListJobsOld(
    const TAuth& auth,
    const TOperationId& operationId,
    const TListJobsOptions& options = TListJobsOptions(),
    IRetryPolicy* retryPolicy = nullptr);

TJobAttributes GetJob(
    const TAuth& auth,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobOptions& options = TGetJobOptions(),
    IRetryPolicy* retryPolicy = nullptr);

TListJobsResult ListJobs(
    const TAuth& auth,
    const TOperationId& operationId,
    const TListJobsOptions& options = TListJobsOptions(),
    IRetryPolicy* retryPolicy = nullptr);

TIntrusivePtr<IFileReader> GetJobInput(
    const TAuth& auth,
    const TJobId& jobId,
    const TGetJobInputOptions& options = TGetJobInputOptions());

TIntrusivePtr<IFileReader> GetJobFailContext(
    const TAuth& auth,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobFailContextOptions& options = TGetJobFailContextOptions());

TString GetJobStderrWithRetries(
    const TAuth& auth,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobStderrOptions& /* options */ = TGetJobStderrOptions(),
    IRetryPolicy* retryPolicy = nullptr);

TIntrusivePtr<IFileReader> GetJobStderr(
    const TAuth& auth,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobStderrOptions& options = TGetJobStderrOptions());

TMaybe<TYPath> GetFileFromCache(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions& options = TGetFileFromCacheOptions(),
    IRetryPolicy* retryPolicy = nullptr);

TYPath PutFileToCache(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options = TPutFileToCacheOptions(),
    IRetryPolicy* retryPolicy = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
