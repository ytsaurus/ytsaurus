#include "job_schema_inference.h"

#include <mapreduce/yt/interface/serialize.h>

#include <mapreduce/yt/raw_client/raw_requests.h>
#include <mapreduce/yt/raw_client/raw_batch_request.h>

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
    , InputSchemasLoaded_(Inputs_.size(), false)
{ }

int TSchemaInferenceContext::GetInputTableCount() const
{
    return static_cast<int>(Inputs_.size());
}

int TSchemaInferenceContext::GetOutputTableCount() const
{
    return static_cast<int>(Outputs_.size());
}

const TVector<TTableSchema>& TSchemaInferenceContext::GetInputTableSchemas() const
{
    NRawClient::TRawBatchRequest batchRequest;
    for (int tableIndex = 0; tableIndex < static_cast<int>(InputSchemas_.size()); ++tableIndex) {
        if (InputSchemasLoaded_[tableIndex]) {
            continue;
        }
        const auto& maybePath = Inputs_[tableIndex].RichYPath;
        Y_VERIFY(maybePath);
        batchRequest.Get(TransactionId_, maybePath->Path_ + "/@schema", TGetOptions{})
            .Subscribe([this, tableIndex] (auto& schemaFuture) {
                Deserialize(InputSchemas_[tableIndex], schemaFuture.GetValue());
            });
    }

    NRawClient::ExecuteBatch(
        RetryPolicy_->CreatePolicyForGenericRequest(),
        Auth_,
        batchRequest,
        TExecuteBatchOptions());

    return InputSchemas_;
}

const TTableSchema& TSchemaInferenceContext::GetInputTableSchema(int index) const
{
    auto& schema = InputSchemas_[index];
    if (!InputSchemasLoaded_[index]) {
        Y_VERIFY(Inputs_[index].RichYPath);
        auto schemaNode = NRawClient::Get(
            RetryPolicy_->CreatePolicyForGenericRequest(),
            Auth_,
            TransactionId_,
            Inputs_[index].RichYPath->Path_ + "/@schema");
        Deserialize(schema, schemaNode);
    }
    return schema;
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
    : Inputs_(std::move(inputs))
    , Outputs_(std::move(outputs))
{
    Y_VERIFY(Inputs_.size() == previousResult.size());
    InputSchemas_.reserve(previousResult.size());
    for (const auto& maybeSchema : previousResult) {
        InputSchemas_.push_back(maybeSchema.GetOrElse(TTableSchema{}));
    }
}

int TSpeculativeSchemaInferenceContext::GetInputTableCount() const
{
    return static_cast<int>(Inputs_.size());
}

int TSpeculativeSchemaInferenceContext::GetOutputTableCount() const
{
    return static_cast<int>(Outputs_.size());
}

const TVector<TTableSchema>& TSpeculativeSchemaInferenceContext::GetInputTableSchemas() const
{
    return InputSchemas_;
}

const TTableSchema& TSpeculativeSchemaInferenceContext::GetInputTableSchema(int index) const
{
    Y_VERIFY(index < static_cast<int>(InputSchemas_.size()));
    return InputSchemas_[index];
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
    const IJob& job,
    const ISchemaInferenceContext& context)
{
    TSchemaInferenceResultBuilder builder(context);
    job.InferSchemas(context, builder);
    return builder.Build();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
