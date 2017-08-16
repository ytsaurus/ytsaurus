#pragma once

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/operation.h>

#include <util/generic/ptr.h>

namespace NYT {

struct TAuth;

namespace NDetail {

class TClient;
using TClientPtr = ::TIntrusivePtr<TClient>;

////////////////////////////////////////////////////////////////////////////////

class TOperation
    : public IOperation
{
public:
    class TOperationImpl;

public:
    TOperation(TOperationId id, TClientPtr client);
    virtual const TOperationId& GetId() const override;
    virtual NThreading::TFuture<void> Watch() override;
    virtual yvector<TFailedJobInfo> GetFailedJobInfo(const TGetFailedJobInfoOptions& options = TGetFailedJobInfoOptions()) override;
    virtual EOperationStatus GetStatus() override;
    virtual TMaybe<TYtError> GetError() override;

private:
    TClientPtr Client_;
    ::TIntrusivePtr<TOperationImpl> Impl_;
};

using TOperationPtr = ::TIntrusivePtr<TOperation>;

////////////////////////////////////////////////////////////////////////////////

TOperationId ExecuteMap(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TMapOperationSpec& spec,
    IJob* mapper,
    const TOperationOptions& options);

TOperationId ExecuteReduce(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TReduceOperationSpec& spec,
    IJob* reducer,
    const TOperationOptions& options);

TOperationId ExecuteJoinReduce(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TJoinReduceOperationSpec& spec,
    IJob* reducer,
    const TOperationOptions& options);

TOperationId ExecuteMapReduce(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TMapReduceOperationSpec& spec,
    IJob* mapper,
    IJob* reduceCombiner,
    IJob* reducer,
    const TMultiFormatDesc& outputMapperDesc,
    const TMultiFormatDesc& inputReduceCombinerDesc,
    const TMultiFormatDesc& outputReduceCombinerDesc,
    const TMultiFormatDesc& inputReducerDesc,
    const TOperationOptions& options);

TOperationId ExecuteSort(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TSortOperationSpec& spec,
    const TOperationOptions& options);

TOperationId ExecuteMerge(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TMergeOperationSpec& spec,
    const TOperationOptions& options);

TOperationId ExecuteErase(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TEraseOperationSpec& spec,
    const TOperationOptions& options);

EOperationStatus CheckOperation(
    const TAuth& auth,
    const TOperationId& operationId);

void WaitForOperation(
    const TAuth& auth,
    const TOperationId& operationId);

void AbortOperation(
    const TAuth& auth,
    const TOperationId& operationId);

////////////////////////////////////////////////////////////////////////////////

TOperationPtr CreateOperationAndWaitIfRequired(const TOperationId& operationId, TClientPtr client, const TOperationOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
