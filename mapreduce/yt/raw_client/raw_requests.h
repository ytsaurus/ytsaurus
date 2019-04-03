#pragma once

#include <mapreduce/yt/interface/fwd.h>
#include <mapreduce/yt/interface/client_method_options.h>
#include <mapreduce/yt/interface/operation.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class IRequestRetryPolicy;
struct TAuth;
struct TExecuteBatchOptions;

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
    IRequestRetryPolicy* retryPolicy = nullptr);

//
// Cypress
//

TNode Get(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options = TGetOptions(),
    IRequestRetryPolicy* retryPolicy = nullptr);

void Set(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options = TSetOptions(),
    IRequestRetryPolicy* retryPolicy = nullptr);

bool Exists(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    IRequestRetryPolicy* retryPolicy = nullptr);

TNodeId Create(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const ENodeType& type,
    const TCreateOptions& options = TCreateOptions(),
    IRequestRetryPolicy* retryPolicy = nullptr);

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
    IRequestRetryPolicy* retryPolicy = nullptr);

TNode::TListType List(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TListOptions& options,
    IRequestRetryPolicy* retryPolicy = nullptr);

TNodeId Link(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options = TLinkOptions(),
    IRequestRetryPolicy* retryPolicy = nullptr);

TLockId Lock(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options = TLockOptions(),
    IRequestRetryPolicy* retryPolicy = nullptr);

void Concatenate(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TVector<TYPath>& sourcePaths,
    const TYPath& destinationPath,
    const TConcatenateOptions& options,
    IRequestRetryPolicy* retryPolicy = nullptr);

//
// Transactions
//

void PingTx(
    const TAuth& auth,
    const TTransactionId& transactionId,
    IRequestRetryPolicy* retryPolicy = nullptr);

//
// Operations
//

TOperationAttributes GetOperation(
    const TAuth& auth,
    const TOperationId& operationId,
    const TGetOperationOptions& options = TGetOperationOptions(),
    IRequestRetryPolicy* retryPolicy = nullptr);

void AbortOperation(
    const TAuth& auth,
    const TOperationId& operationId);

void CompleteOperation(
    const TAuth& auth,
    const TOperationId& operationId);

void UpdateOperationParameters(
    const TAuth& auth,
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options,
    IRequestRetryPolicy* retryPolicy = nullptr);

TListOperationsResult ListOperations(
    const TAuth& auth,
    const TListOperationsOptions& options = TListOperationsOptions(),
    IRequestRetryPolicy* retryPolicy = nullptr);

//
// Jobs
//

TJobAttributes GetJob(
    const TAuth& auth,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobOptions& options = TGetJobOptions(),
    IRequestRetryPolicy* retryPolicy = nullptr);

TListJobsResult ListJobs(
    const TAuth& auth,
    const TOperationId& operationId,
    const TListJobsOptions& options = TListJobsOptions(),
    IRequestRetryPolicy* retryPolicy = nullptr);

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
    IRequestRetryPolicy* retryPolicy = nullptr);

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
    IRequestRetryPolicy* retryPolicy = nullptr);

TYPath PutFileToCache(
    const TAuth& auth,
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options = TPutFileToCacheOptions(),
    IRequestRetryPolicy* retryPolicy = nullptr);

//
// Tables
//

void AlterTable(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TAlterTableOptions& options,
    IRequestRetryPolicy* retryPolicy = nullptr);

void AlterTableReplica(
    const TAuth& auth,
    const TReplicaId& replicaId,
    const TAlterTableReplicaOptions& options,
    IRequestRetryPolicy* retryPolicy = nullptr);

void DeleteRows(
    const TAuth& auth,
    const TYPath& path,
    const TNode::TListType& keys,
    const TDeleteRowsOptions& options,
    IRequestRetryPolicy* retryPolicy = nullptr);

void EnableTableReplica(
    const TAuth& auth,
    const TReplicaId& replicaId,
    IRequestRetryPolicy* retryPolicy = nullptr);

void DisableTableReplica(
    const TAuth& auth,
    const TReplicaId& replicaId,
    IRequestRetryPolicy* retryPolicy = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail::NRawClient
} // namespace NYT
