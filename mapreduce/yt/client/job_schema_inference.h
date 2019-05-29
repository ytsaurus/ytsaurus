#pragma once

#include "structured_table_formats.h"

#include <mapreduce/yt/interface/operation.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

class TSchemaInferenceContext
    : public ISchemaInferenceContext
{
public:
    TSchemaInferenceContext(
        TStructuredJobTableList structuredInputs,
        TStructuredJobTableList structuredOutputs,
        const TAuth& auth,
        const IClientRetryPolicyPtr& retryPolicy,
        TTransactionId transactionId);

    int GetInputTableCount() const override;
    int GetOutputTableCount() const override;

    const TTableSchema& GetInputTableSchema(int index) const override;
    TMaybe<TYPath> GetInputTablePath(int index) const override;

    TMaybe<TYPath> GetOutputTablePath(int index) const override;

private:
    TStructuredJobTableList Inputs_;
    TStructuredJobTableList Outputs_;
    const TAuth& Auth_;
    const IClientRetryPolicyPtr RetryPolicy_;
    TTransactionId TransactionId_;

    mutable TVector<TTableSchema> InputSchemas_;
};

////////////////////////////////////////////////////////////////////////////////

class TSpeculativeSchemaInferenceContext
    : public ISchemaInferenceContext
{
public:
    TSpeculativeSchemaInferenceContext(
        const TSchemaInferenceResult& previousResult,
        TStructuredJobTableList inputs,
        TStructuredJobTableList outputs);

    int GetInputTableCount() const override;
    int GetOutputTableCount() const override;

    const TTableSchema& GetInputTableSchema(int index) const override;
    TMaybe<TYPath> GetInputTablePath(int index) const override;

    TMaybe<TYPath> GetOutputTablePath(int index) const override;

private:
    TSchemaInferenceResult PreviousResult_;
    TStructuredJobTableList Inputs_;
    TStructuredJobTableList Outputs_;
};

////////////////////////////////////////////////////////////////////////////////

TSchemaInferenceResult InferJobSchemas(
    const IStructuredJob& job,
    const ISchemaInferenceContext& context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
