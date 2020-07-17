#include "prepare_operation.h"

#include <mapreduce/yt/interface/serialize.h>

#include <mapreduce/yt/raw_client/raw_requests.h>
#include <mapreduce/yt/raw_client/raw_batch_request.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

TOperationPreparationContext::TOperationPreparationContext(
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

int TOperationPreparationContext::GetInputCount() const
{
    return static_cast<int>(Inputs_.size());
}

int TOperationPreparationContext::GetOutputCount() const
{
    return static_cast<int>(Outputs_.size());
}

const TVector<TTableSchema>& TOperationPreparationContext::GetInputSchemas() const
{
    TVector<NThreading::TFuture<TNode>> schemaFutures;
    NRawClient::TRawBatchRequest batch;
    for (int tableIndex = 0; tableIndex < static_cast<int>(InputSchemas_.size()); ++tableIndex) {
        if (InputSchemasLoaded_[tableIndex]) {
            schemaFutures.emplace_back();
            continue;
        }
        const auto& maybePath = Inputs_[tableIndex].RichYPath;
        Y_VERIFY(maybePath);
        schemaFutures.push_back(batch.Get(TransactionId_, maybePath->Path_ + "/@schema", TGetOptions{}));
    }

    NRawClient::ExecuteBatch(
        RetryPolicy_->CreatePolicyForGenericRequest(),
        Auth_,
        batch);

    for (int tableIndex = 0; tableIndex < static_cast<int>(InputSchemas_.size()); ++tableIndex) {
        if (schemaFutures[tableIndex].Initialized()) {
            Deserialize(InputSchemas_[tableIndex], schemaFutures[tableIndex].ExtractValueSync());
        }
    }

    return InputSchemas_;
}

const TTableSchema& TOperationPreparationContext::GetInputSchema(int index) const
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

TMaybe<TYPath> TOperationPreparationContext::GetInputPath(int index) const
{
    Y_VERIFY(index < static_cast<int>(Inputs_.size()));
    if (Inputs_[index].RichYPath) {
        return Inputs_[index].RichYPath->Path_;
    }
    return Nothing();
}

TMaybe<TYPath> TOperationPreparationContext::GetOutputPath(int index) const
{
    Y_VERIFY(index < static_cast<int>(Outputs_.size()));
    if (Outputs_[index].RichYPath) {
        return Outputs_[index].RichYPath->Path_;
    }
    return Nothing();
}

////////////////////////////////////////////////////////////////////////////////

TSpeculativeOperationPreparationContext::TSpeculativeOperationPreparationContext(
    const TVector<TTableSchema>& previousResult,
    TStructuredJobTableList inputs,
    TStructuredJobTableList outputs)
    : InputSchemas_(previousResult)
    , Inputs_(std::move(inputs))
    , Outputs_(std::move(outputs))
{
    Y_VERIFY(Inputs_.size() == previousResult.size());
}

int TSpeculativeOperationPreparationContext::GetInputCount() const
{
    return static_cast<int>(Inputs_.size());
}

int TSpeculativeOperationPreparationContext::GetOutputCount() const
{
    return static_cast<int>(Outputs_.size());
}

const TVector<TTableSchema>& TSpeculativeOperationPreparationContext::GetInputSchemas() const
{
    return InputSchemas_;
}

const TTableSchema& TSpeculativeOperationPreparationContext::GetInputSchema(int index) const
{
    Y_VERIFY(index < static_cast<int>(InputSchemas_.size()));
    return InputSchemas_[index];
}

TMaybe<TYPath> TSpeculativeOperationPreparationContext::GetInputPath(int index) const
{
    Y_VERIFY(index < static_cast<int>(Inputs_.size()));
    if (Inputs_[index].RichYPath) {
        return Inputs_[index].RichYPath->Path_;
    }
    return Nothing();
}

TMaybe<TYPath> TSpeculativeOperationPreparationContext::GetOutputPath(int index) const
{
    Y_VERIFY(index < static_cast<int>(Outputs_.size()));
    if (Outputs_[index].RichYPath) {
        return Outputs_[index].RichYPath->Path_;
    }
    return Nothing();
}

////////////////////////////////////////////////////////////////////////////////

TVector<TTableSchema> PrepareOperation(
    const IJob& job,
    const IOperationPreparationContext& context,
    TStructuredJobTableList* inputsPtr,
    TStructuredJobTableList* outputsPtr,
    TUserJobFormatHints& hints)
{
    TJobOperationPreparer preparer(context);
    job.PrepareOperation(context, preparer);
    preparer.Finish();

    const auto& inputDescriptions = preparer.GetInputDescriptions();
    const auto& columnRenamings = preparer.GetInputColumnRenamings();
    const auto& columnFilters = preparer.GetInputColumnFilters();
    if (inputsPtr) {
        auto& inputs = *inputsPtr;
        for (int i = 0; i < static_cast<int>(inputs.size()); ++i) {
            Y_VERIFY(inputs[i].RichYPath);
            if (inputDescriptions[i] && HoldsAlternative<TUnspecifiedTableStructure>(inputs[i].Description)) {
                inputs[i].Description = *inputDescriptions[i];
            }
            if (!columnRenamings[i].empty()) {
                inputs[i].RichYPath->RenameColumns(columnRenamings[i]);
            }
            if (columnFilters[i]) {
                inputs[i].RichYPath->Columns(*columnFilters[i]);
            }
        }
    }

    if (outputsPtr) {
        auto& outputs = *outputsPtr;
        const auto& outputDescriptions = preparer.GetOutputDescriptions();
        for (int i = 0; i < static_cast<int>(outputs.size()); ++i) {
            Y_VERIFY(outputs[i].RichYPath);
            if (outputDescriptions[i] && HoldsAlternative<TUnspecifiedTableStructure>(outputs[i].Description)) {
                outputs[i].Description = *outputDescriptions[i];
            }
        }
    }

    auto applyPatch = [](TMaybe<TFormatHints>& origin, const TMaybe<TFormatHints>& patch) {
        if (origin) {
            if (patch) {
                origin->Merge(*patch);
            }
        } else {
            origin = patch;
        }
    };

    auto preparerHints = preparer.GetFormatHints();
    applyPatch(preparerHints.InputFormatHints_, hints.InputFormatHints_);
    applyPatch(preparerHints.OutputFormatHints_, hints.OutputFormatHints_);
    hints = std::move(preparerHints);

    return preparer.GetOutputSchemas();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
