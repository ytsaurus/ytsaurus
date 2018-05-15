#pragma once

#include <mapreduce/yt/interface/fwd.h>
#include <mapreduce/yt/interface/client_method_options.h>

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

//
// marks `batchRequest' as executed
void ExecuteBatch(
    const TAuth& auth,
    TRawBatchRequest& batchRequest,
    const TExecuteBatchOptions& options,
    IRetryPolicy& retryPolicy);

// TODO: use retry policy here
TNode Get(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options = TGetOptions());

void Set(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options = TSetOptions());

bool Exists(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path);

TNodeId Create(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const ENodeType& type,
    const TCreateOptions& options = TCreateOptions());

void Remove(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TRemoveOptions& options = TRemoveOptions());

TNode::TListType List(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TListOptions& options);

TNodeId Link(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options = TLinkOptions());

TLockId Lock(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options = TLockOptions());

TNode GetOperation(
    const TAuth& auth,
    const TOperationId& operationId,
    const TGetOperationOptions& options = TGetOperationOptions());

TNode ListJobs(
    const TAuth& auth,
    const TOperationId& operationId,
    const TListJobsOptions& options = TListJobsOptions());

TString GetJobStderr(
    const TAuth& auth,
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobStderrOptions& options = TGetJobStderrOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
