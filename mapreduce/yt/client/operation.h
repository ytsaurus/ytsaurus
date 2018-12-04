#pragma once

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/operation.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NYT {

class TPingableTransaction;
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
    virtual TVector<TFailedJobInfo> GetFailedJobInfo(const TGetFailedJobInfoOptions& options = TGetFailedJobInfoOptions()) override;
    virtual EOperationBriefState GetBriefState() override;
    virtual TMaybe<TYtError> GetError() override;
    virtual TJobStatistics GetJobStatistics() override;
    virtual TMaybe<TOperationBriefProgress> GetBriefProgress() override;
    virtual void AbortOperation() override;
    virtual void CompleteOperation() override;
    virtual TOperationAttributes GetAttributes(const TGetOperationOptions& options) override;
    virtual void UpdateParameters(const TUpdateOperationParametersOptions& options) override;
    virtual TJobAttributes GetJob(const TJobId& jobId, const TGetJobOptions& options) override;
    virtual TListJobsResult ListJobs(const TListJobsOptions& options) override;

private:
    TClientPtr Client_;
    ::TIntrusivePtr<TOperationImpl> Impl_;
};

using TOperationPtr = ::TIntrusivePtr<TOperation>;

////////////////////////////////////////////////////////////////////////////////

class TOperationPreparer
{
public:
    TOperationPreparer(TClientPtr client, TTransactionId transactionId);

    const TAuth& GetAuth() const;
    TTransactionId GetTransactionId() const;

    TOperationId StartOperation(
        const TString& operationType,
        const TString& ysonSpec,
        bool useStartOperationRequest = false);

private:
    TClientPtr Client_;
    TTransactionId TransactionId_;
};

////////////////////////////////////////////////////////////////////////////////

TOperationId ExecuteMap(
    TOperationPreparer& preparer,
    const TMapOperationSpec& spec,
    const IStructuredJob& mapper,
    const TOperationOptions& options);

TOperationId ExecuteRawMap(
    TOperationPreparer& preparer,
    const TRawMapOperationSpec& spec,
    const IRawJob& mapper,
    const TOperationOptions& options);

TOperationId ExecuteReduce(
    TOperationPreparer& preparer,
    const TReduceOperationSpec& spec,
    const IStructuredJob& reducer,
    const TOperationOptions& options);

TOperationId ExecuteRawReduce(
    TOperationPreparer& preparer,
    const TRawReduceOperationSpec& spec,
    const IRawJob& reducer,
    const TOperationOptions& options);

TOperationId ExecuteJoinReduce(
    TOperationPreparer& preparer,
    const TJoinReduceOperationSpec& spec,
    const IStructuredJob& reducer,
    const TOperationOptions& options);

TOperationId ExecuteRawJoinReduce(
    TOperationPreparer& preparer,
    const TRawJoinReduceOperationSpec& spec,
    const IRawJob& reducer,
    const TOperationOptions& options);

TOperationId ExecuteMapReduce(
    TOperationPreparer& preparer,
    const TMapReduceOperationSpec& spec,
    const IStructuredJob* mapper,
    const IStructuredJob* reduceCombiner,
    const IStructuredJob& reducer,
    const TMultiFormatDesc& outputMapperDesc,
    const TMultiFormatDesc& inputReduceCombinerDesc,
    const TMultiFormatDesc& outputReduceCombinerDesc,
    const TMultiFormatDesc& inputReducerDesc,
    const TOperationOptions& options);

TOperationId ExecuteRawMapReduce(
    TOperationPreparer& preparer,
    const TRawMapReduceOperationSpec& spec,
    const IRawJob* mapper,
    const IRawJob* reduceCombiner,
    const IRawJob& reducer,
    const TOperationOptions& options);

TOperationId ExecuteSort(
    TOperationPreparer& preparer,
    const TSortOperationSpec& spec,
    const TOperationOptions& options);

TOperationId ExecuteMerge(
    TOperationPreparer& preparer,
    const TMergeOperationSpec& spec,
    const TOperationOptions& options);

TOperationId ExecuteErase(
    TOperationPreparer& preparer,
    const TEraseOperationSpec& spec,
    const TOperationOptions& options);

TOperationId ExecuteVanilla(
    TOperationPreparer& preparer,
    const TVanillaOperationSpec& spec,
    const TOperationOptions& options);

EOperationBriefState CheckOperation(
    const TAuth& auth,
    const TOperationId& operationId);

void WaitForOperation(
    const TAuth& auth,
    const TOperationId& operationId);

void AbortOperation(
    const TAuth& auth,
    const TOperationId& operationId);

void CompleteOperation(
    const TAuth& auth,
    const TOperationId& operationId);

////////////////////////////////////////////////////////////////////////////////

TOperationPtr CreateOperationAndWaitIfRequired(const TOperationId& operationId, TClientPtr client, const TOperationOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
