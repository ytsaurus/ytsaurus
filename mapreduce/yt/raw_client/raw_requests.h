#pragma once

#include <mapreduce/yt/interface/fwd.h>
#include <mapreduce/yt/interface/client_method_options.h>
#include <mapreduce/yt/interface/operation.h>
#include <mapreduce/yt/interface/retry_policy.h>

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
    IRequestRetryPolicyPtr retryPolicy = nullptr);

//
// Cypress
//

TNode Get(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options = TGetOptions(),
    IRequestRetryPolicyPtr retryPolicy = nullptr);

void Set(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options = TSetOptions(),
    IRequestRetryPolicyPtr retryPolicy = nullptr);

bool Exists(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

TNodeId Create(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const ENodeType& type,
    const TCreateOptions& options = TCreateOptions(),
    IRequestRetryPolicyPtr retryPolicy = nullptr);

TNodeId Copy(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

TNodeId Move(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

void Remove(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TRemoveOptions& options = TRemoveOptions(),
    IRequestRetryPolicyPtr retryPolicy = nullptr);

TNode::TListType List(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TListOptions& options,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

TNodeId Link(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options = TLinkOptions(),
    IRequestRetryPolicyPtr retryPolicy = nullptr);

TLockId Lock(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options = TLockOptions(),
    IRequestRetryPolicyPtr retryPolicy = nullptr);

void Unlock(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TUnlockOptions& options = TUnlockOptions(),
    IRequestRetryPolicyPtr retryPolicy = nullptr);

void Concatenate(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TVector<TYPath>& sourcePaths,
    const TYPath& destinationPath,
    const TConcatenateOptions& options,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

//
// Transactions
//

void PingTx(
    const TAuth& auth,
    const TTransactionId& transactionId,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

//
// Operations
//

TOperationAttributes GetOperation(
    const TAuth& auth,
    const TOperationId& operationId,
    const TGetOperationOptions& options = TGetOperationOptions(),
    IRequestRetryPolicyPtr retryPolicy = nullptr);

void AbortOperation(
    const TAuth& auth,
    const TOperationId& operationId,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

void CompleteOperation(
    const TAuth& auth,
    const TOperationId& operationId,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

TListOperationsResult ListOperations(
    const TAuth& auth,
    const TListOperationsOptions& options = TListOperationsOptions(),
    IRequestRetryPolicyPtr retryPolicy = nullptr);

void UpdateOperationParameters(
    const TAuth& auth,
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

//
// Jobs
//

TJobAttributes GetJob(
    const TAuth& auth,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobOptions& options = TGetJobOptions(),
    IRequestRetryPolicyPtr retryPolicy = nullptr);

TListJobsResult ListJobs(
    const TAuth& auth,
    const TOperationId& operationId,
    const TListJobsOptions& options = TListJobsOptions(),
    IRequestRetryPolicyPtr retryPolicy = nullptr);

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
    IRequestRetryPolicyPtr retryPolicy = nullptr);

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
    IRequestRetryPolicyPtr retryPolicy = nullptr);

TYPath PutFileToCache(
    const TAuth& auth,
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options = TPutFileToCacheOptions(),
    IRequestRetryPolicyPtr retryPolicy = nullptr);

//
// Tables
//

void AlterTable(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TAlterTableOptions& options,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

void AlterTableReplica(
    const TAuth& auth,
    const TReplicaId& replicaId,
    const TAlterTableReplicaOptions& options,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

void DeleteRows(
    const TAuth& auth,
    const TYPath& path,
    const TNode::TListType& keys,
    const TDeleteRowsOptions& options,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

void EnableTableReplica(
    const TAuth& auth,
    const TReplicaId& replicaId,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

void DisableTableReplica(
    const TAuth& auth,
    const TReplicaId& replicaId,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

void FreezeTable(
    const TAuth& auth,
    const TYPath& path,
    const TFreezeTableOptions& options,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

void UnfreezeTable(
    const TAuth& auth,
    const TYPath& path,
    const TUnfreezeTableOptions& options,
    IRequestRetryPolicyPtr retryPolicy = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail::NRawClient
} // namespace NYT
