#pragma once

#include <mapreduce/yt/interface/fwd.h>
#include <mapreduce/yt/interface/client_method_options.h>
#include <mapreduce/yt/interface/operation.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class IRetryPolicy;
struct TAuth;
struct TExecuteBatchOptions;
namespace NDetail {
    struct IRetryPolicy;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail::NRawClient {

////////////////////////////////////////////////////////////////////////////////

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

//
// Cypress
//

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

TNodeId Copy(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options);

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

void Concatenate(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TVector<TYPath>& sourcePaths,
    const TYPath& destinationPath,
    const TConcatenateOptions& options,
    IRetryPolicy* retryPolicy = nullptr);

//
// Transactions
//

void PingTx(
    const TAuth& auth,
    const TTransactionId& transactionId,
    IRetryPolicy* retryPolicy = nullptr);

//
// Operations
//

TOperationAttributes GetOperation(
    const TAuth& auth,
    const TOperationId& operationId,
    const TGetOperationOptions& options = TGetOperationOptions(),
    IRetryPolicy* retryPolicy = nullptr);

void AbortOperation(
    const TAuth& auth,
    const TOperationId& operationId);

void CompleteOperation(
    const TAuth& auth,
    const TOperationId& operationId);

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

//
// Jobs
//

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

//
// File cache
//

TMaybe<TYPath> GetFileFromCache(
    const TAuth& auth,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions& options = TGetFileFromCacheOptions(),
    IRetryPolicy* retryPolicy = nullptr);

TYPath PutFileToCache(
    const TAuth& auth,
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options = TPutFileToCacheOptions(),
    IRetryPolicy* retryPolicy = nullptr);

//
// Tables
//

void AlterTable(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TAlterTableOptions& options,
    IRetryPolicy* retryPolicy = nullptr);

void AlterTableReplica(
    const TAuth& auth,
    const TReplicaId& replicaId,
    const TAlterTableReplicaOptions& options,
    IRetryPolicy* retryPolicy = nullptr);

void DeleteRows(
    const TAuth& auth,
    const TYPath& path,
    const TNode::TListType& keys,
    const TDeleteRowsOptions& options,
    IRetryPolicy* retryPolicy = nullptr);

void EnableTableReplica(
    const TAuth& auth,
    const TReplicaId& replicaId,
    IRetryPolicy* retryPolicy = nullptr);

void DisableTableReplica(
    const TAuth& auth,
    const TReplicaId& replicaId,
    IRetryPolicy* retryPolicy = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail::NRawClient
} // namespace NYT
