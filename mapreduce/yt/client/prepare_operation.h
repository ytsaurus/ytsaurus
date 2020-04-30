#pragma once

#include "structured_table_formats.h"

#include <mapreduce/yt/interface/operation.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

class TOperationPreparationContext
    : public IOperationPreparationContext
{
public:
    TOperationPreparationContext(
        TStructuredJobTableList structuredInputs,
        TStructuredJobTableList structuredOutputs,
        const TAuth& auth,
        const IClientRetryPolicyPtr& retryPolicy,
        TTransactionId transactionId);

    int GetInputCount() const override;
    int GetOutputCount() const override;

    const TVector<TTableSchema>& GetInputSchemas() const override;
    const TTableSchema& GetInputSchema(int index) const override;

    TMaybe<TYPath> GetInputPath(int index) const override;
    TMaybe<TYPath> GetOutputPath(int index) const override;

private:
    TStructuredJobTableList Inputs_;
    TStructuredJobTableList Outputs_;
    const TAuth& Auth_;
    const IClientRetryPolicyPtr RetryPolicy_;
    TTransactionId TransactionId_;

    mutable TVector<TTableSchema> InputSchemas_;
    mutable TVector<bool> InputSchemasLoaded_;
};

////////////////////////////////////////////////////////////////////////////////

class TSpeculativeOperationPreparationContext
    : public IOperationPreparationContext
{
public:
    TSpeculativeOperationPreparationContext(
        const TVector<TTableSchema>& previousResult,
        TStructuredJobTableList inputs,
        TStructuredJobTableList outputs);

    int GetInputCount() const override;
    int GetOutputCount() const override;

    const TVector<TTableSchema>& GetInputSchemas() const override;
    const TTableSchema& GetInputSchema(int index) const override;

    TMaybe<TYPath> GetInputPath(int index) const override;
    TMaybe<TYPath> GetOutputPath(int index) const override;

private:
    TVector<TTableSchema> InputSchemas_;
    TStructuredJobTableList Inputs_;
    TStructuredJobTableList Outputs_;
};

////////////////////////////////////////////////////////////////////////////////

TVector<TTableSchema> PrepareOperation(
    const IJob& job,
    const IOperationPreparationContext& context,
    TStructuredJobTableList* inputs,
    TStructuredJobTableList* outputs,
    TUserJobFormatHints& hints);

////////////////////////////////////////////////////////////////////////////////

TJobOperationPreparer GetOperationPreparer(
    const IJob& job,
    const IOperationPreparationContext& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
