#include "job_schema_inference.h"

#include <mapreduce/yt/interface/serialize.h>

#include <mapreduce/yt/raw_client/raw_requests.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

TSchemaInferenceContext::TSchemaInferenceContext(
    TStructuredJobTableList structuredInputs,
    TStructuredJobTableList structuredOutputs,
    const TAuth& auth,
    const IClientRetryPolicyPtr& retryPolicy,
    TTransactionId transactionId)
    : Inputs_(std::move(structuredInputs))
    , Outputs_(std::move(structuredOutputs))
    , Auth_(auth)
    , RetryPolicy_(retryPolicy)
    , TransactionId_(transactionId)
    , InputSchemas_(Inputs_.size())
{ }

int TSchemaInferenceContext::GetInputTableCount() const
{
    return static_cast<int>(Inputs_.size());
}

int TSchemaInferenceContext::GetOutputTableCount() const
{
    return static_cast<int>(Outputs_.size());
}

const TTableSchema& TSchemaInferenceContext::GetInputTableSchema(int index) const
{
    auto& maybeSchema = InputSchemas_[index];
    if (maybeSchema.Empty()) {
        Y_VERIFY(Inputs_[index].RichYPath);
        auto schemaNode = NRawClient::Get(
            RetryPolicy_->CreatePolicyForGenericRequest(),
            Auth_,
            TransactionId_,
            Inputs_[index].RichYPath->Path_ + "/@schema");
        Deserialize(maybeSchema, schemaNode);
    }
    return maybeSchema;
}

TMaybe<TYPath> TSchemaInferenceContext::GetInputTablePath(int index) const
{
    Y_VERIFY(index < static_cast<int>(Inputs_.size()));
    if (Inputs_[index].RichYPath) {
        return Inputs_[index].RichYPath->Path_;
    }
    return Nothing();
}

TMaybe<TYPath> TSchemaInferenceContext::GetOutputTablePath(int index) const
{
    Y_VERIFY(index < static_cast<int>(Outputs_.size()));
    if (Outputs_[index].RichYPath) {
        return Outputs_[index].RichYPath->Path_;
    }
    return Nothing();
}

////////////////////////////////////////////////////////////////////////////////

TSpeculativeSchemaInferenceContext::TSpeculativeSchemaInferenceContext(
    const TSchemaInferenceResult& previousResult,
    TStructuredJobTableList inputs,
    TStructuredJobTableList outputs)
    : PreviousResult_(previousResult)
    , Inputs_(std::move(inputs))
    , Outputs_(std::move(outputs))
{
    Y_VERIFY(Inputs_.size() == PreviousResult_.size());
}

int TSpeculativeSchemaInferenceContext::GetInputTableCount() const
{
    return static_cast<int>(Inputs_.size());
}

int TSpeculativeSchemaInferenceContext::GetOutputTableCount() const
{
    return static_cast<int>(Outputs_.size());
}

const TTableSchema& TSpeculativeSchemaInferenceContext::GetInputTableSchema(int index) const
{
    Y_VERIFY(index < static_cast<int>(PreviousResult_.size()));
    return PreviousResult_[index].GetOrElse(TTableSchema{});
}

TMaybe<TYPath> TSpeculativeSchemaInferenceContext::GetInputTablePath(int index) const
{
    Y_VERIFY(index < static_cast<int>(Inputs_.size()));
    if (Inputs_[index].RichYPath) {
        return Inputs_[index].RichYPath->Path_;
    }
    return Nothing();
}

TMaybe<TYPath> TSpeculativeSchemaInferenceContext::GetOutputTablePath(int index) const
{
    Y_VERIFY(index < static_cast<int>(Outputs_.size()));
    if (Outputs_[index].RichYPath) {
        return Outputs_[index].RichYPath->Path_;
    }
    return Nothing();
}

////////////////////////////////////////////////////////////////////////////////

TSchemaInferenceResult InferJobSchemas(
    const IStructuredJob& job,
    const ISchemaInferenceContext& context)
{
    TSchemaInferenceResultBuilder builder(context);
    job.InferSchemas(context, builder);
    return builder.Build();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
