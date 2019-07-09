#include "operation.h"

#include <util/generic/iterator_range.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
    i64 OutputTableCount = -1;
} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

const TVector<TStructuredTablePath>& TOperationIOSpecBase::GetStructuredInputs() const
{
    return StructuredInputs_;
}

const TVector<TStructuredTablePath>& TOperationIOSpecBase::GetStructuredOutputs() const
{
    return StructuredOutputs_;
}

void TOperationIOSpecBase::AddStructuredInput(const TStructuredTablePath& path)
{
    Inputs_.push_back(path.RichYPath);
    StructuredInputs_.push_back(path);
}

void TOperationIOSpecBase::AddStructuredOutput(const TStructuredTablePath& path)
{
    Outputs_.push_back(path.RichYPath);
    StructuredOutputs_.push_back(path);
}

////////////////////////////////////////////////////////////////////////////////

TRawJobContext::TRawJobContext(size_t outputTableCount)
    : InputFile_(Duplicate(0))
{
    for (size_t i = 0; i != outputTableCount; ++i) {
        OutputFileList_.emplace_back(Duplicate(3 * i + 1));
    }
}

const TFile& TRawJobContext::GetInputFile() const
{
    return InputFile_;
}

const TVector<TFile>& TRawJobContext::GetOutputFileList() const
{
    return OutputFileList_;
}

////////////////////////////////////////////////////////////////////////////////

TUserJobSpec& TUserJobSpec::AddLocalFile(
    const TLocalFilePath& path,
    const TAddLocalFileOptions& options)
{
    LocalFiles_.emplace_back(path, options);
    return *this;
}

TUserJobSpec& TUserJobSpec::JobBinaryLocalPath(TString path, TMaybe<TString> md5)
{
    JobBinary_ = TJobBinaryLocalPath{path, md5};
    return *this;
}

TUserJobSpec& TUserJobSpec::JobBinaryCypressPath(TString path)
{
    JobBinary_ = TJobBinaryCypressPath{path};
    return *this;
}

const TJobBinaryConfig& TUserJobSpec::GetJobBinary() const
{
    return JobBinary_;
}

TVector<std::tuple<TLocalFilePath, TAddLocalFileOptions>> TUserJobSpec::GetLocalFiles() const
{
    return LocalFiles_;
}

////////////////////////////////////////////////////////////////////////////////

TSchemaInferenceResultBuilder::TSchemaInferenceResultBuilder(const ISchemaInferenceContext& context)
    : Context_(context)
    , Schemas_(context.GetOutputTableCount(), TIllegallyMissingSchema{})
{ }

TSchemaInferenceResultBuilder& TSchemaInferenceResultBuilder::OutputSchema(int tableIndex, TTableSchema schema)
{
    Y_ENSURE(tableIndex < static_cast<int>(Schemas_.size()));
    ValidateIllegallyMissing(tableIndex);
    Schemas_[tableIndex] = std::move(schema);
    return *this;
}

TSchemaInferenceResultBuilder& TSchemaInferenceResultBuilder::OutputSchemas(int begin, int end, const TTableSchema& schema)
{
    Y_ENSURE(begin <= end);
    for (auto i = begin; i < end; ++i) {
        ValidateIllegallyMissing(i);
    }
    for (auto i = begin; i < end; ++i) {
        Schemas_[i] = schema;
    }
    return *this;
}

TSchemaInferenceResultBuilder& TSchemaInferenceResultBuilder::IntentionallyMissingOutputSchema(int tableIndex)
{
    Y_ENSURE(tableIndex < static_cast<int>(Schemas_.size()));
    ValidateIllegallyMissing(tableIndex);
    Schemas_[tableIndex] = TIntentionallyMissingSchema{};
    return *this;
}

TSchemaInferenceResultBuilder& TSchemaInferenceResultBuilder::RemainingOutputSchemas(const TTableSchema& schema)
{
    for (auto& entry : Schemas_) {
        if (HoldsAlternative<TIllegallyMissingSchema>(entry)) {
            entry = schema;
        }
    }
    return *this;
}

TSchemaInferenceResult TSchemaInferenceResultBuilder::Build()
{
    FinallyValidate();

    TSchemaInferenceResult result;
    result.reserve(Schemas_.size());
    for (auto& schema : Schemas_) {
        if (HoldsAlternative<TTableSchema>(schema)) {
            result.push_back(std::move(Get<TTableSchema>(schema)));
        } else {
            result.emplace_back();
        }
        schema = TIllegallyMissingSchema();
    }
    return result;
}

void TSchemaInferenceResultBuilder::FinallyValidate() const
{
    TVector<int> illegallyMissingSchemaIndices;
    for (int i = 0; i < static_cast<int>(Schemas_.size()); ++i) {
        if (HoldsAlternative<TIllegallyMissingSchema>(Schemas_[i])) {
            illegallyMissingSchemaIndices.push_back(i);
        }
    }
    if (illegallyMissingSchemaIndices.empty()) {
        return;
    }
    TApiUsageError error;
    error << "Output table schemas are missing and not marked as intentionally missing: ";
    for (auto i : illegallyMissingSchemaIndices) {
        error << "no. " << i;
        if (auto path = Context_.GetInputTablePath(i)) {
            error << "(" << *path << ")";
        }
        error << "; ";
    }
    ythrow error;
}

void TSchemaInferenceResultBuilder::ValidateIllegallyMissing(int tableIndex) const
{
    Y_ENSURE_EX(HoldsAlternative<TIllegallyMissingSchema>(Schemas_[tableIndex]),
        TApiUsageError() <<
        "Output table schema no. " << tableIndex << " " <<
        "(" << Context_.GetOutputTablePath(tableIndex).GetOrElse("<unknown path>") << ") " <<
        "is already set or marked as intentionally missing");
}

////////////////////////////////////////////////////////////////////////////////

void IJob::InferSchemas(const ISchemaInferenceContext& context, TSchemaInferenceResultBuilder& resultBuilder) const
{
    for (int i = 0; i < context.GetOutputTableCount(); ++i) {
        resultBuilder.IntentionallyMissingOutputSchema(i);
    }
}

////////////////////////////////////////////////////////////////////////////////

IOperationPtr IOperationClient::Map(
    const TMapOperationSpec& spec,
    ::TIntrusivePtr<IMapperBase> mapper,
    const TOperationOptions& options)
{
    Y_VERIFY(mapper.Get());

    return DoMap(
        spec,
        *mapper,
        options);
}

IOperationPtr IOperationClient::Map(
    const TOneOrMany<TStructuredTablePath>& input,
    const TOneOrMany<TStructuredTablePath>& output,
    ::TIntrusivePtr<IMapperBase> mapper,
    const TMapOperationSpec& spec,
    const TOperationOptions& options)
{
    Y_ENSURE_EX(spec.Inputs_.empty(),
        TApiUsageError() << "TMapOperationSpec::Inputs MUST be empty");
    Y_ENSURE_EX(spec.Outputs_.empty(),
        TApiUsageError() << "TMapOperationSpec::Outputs MUST be empty");

    auto mapSpec = spec;
    for (const auto& inputPath : input.Parts_)
        mapSpec.AddStructuredInput(inputPath);
    for (const auto& outputPath : output.Parts_)
        mapSpec.AddStructuredOutput(outputPath);
    return Map(mapSpec, mapper, options);
}

IOperationPtr IOperationClient::Reduce(
    const TReduceOperationSpec& spec,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOperationOptions& options)
{
    Y_VERIFY(reducer.Get());

    return DoReduce(
        spec,
        *reducer,
        options);
}

IOperationPtr IOperationClient::Reduce(
    const TOneOrMany<TStructuredTablePath>& input,
    const TOneOrMany<TStructuredTablePath>& output,
    const TKeyColumns& reduceBy,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TReduceOperationSpec& spec,
    const TOperationOptions& options)
{
    Y_ENSURE_EX(spec.Inputs_.empty(),
        TApiUsageError() << "TReduceOperationSpec::Inputs MUST be empty");
    Y_ENSURE_EX(spec.Outputs_.empty(),
        TApiUsageError() << "TReduceOperationSpec::Outputs MUST be empty");
    Y_ENSURE_EX(spec.ReduceBy_.Parts_.empty(),
        TApiUsageError() << "TReduceOperationSpec::ReduceBy MUST be empty");

    auto reduceSpec = spec;
    for (const auto& inputPath : input.Parts_)
        reduceSpec.AddStructuredInput(inputPath);
    for (const auto& outputPath : output.Parts_)
        reduceSpec.AddStructuredOutput(outputPath);
    reduceSpec.ReduceBy(reduceBy);
    return Reduce(reduceSpec, reducer, options);
}

IOperationPtr IOperationClient::JoinReduce(
    const TJoinReduceOperationSpec& spec,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOperationOptions& options)
{
    Y_VERIFY(reducer.Get());

    return DoJoinReduce(
        spec,
        *reducer,
        options);
}

IOperationPtr IOperationClient::MapReduce(
    const TMapReduceOperationSpec& spec,
    ::TIntrusivePtr<IMapperBase> mapper,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOperationOptions& options)
{
    Y_VERIFY(reducer.Get());

    return DoMapReduce(
        spec,
        mapper.Get(),
        nullptr,
        *reducer,
        options);
}

IOperationPtr IOperationClient::MapReduce(
    const TMapReduceOperationSpec& spec,
    ::TIntrusivePtr<IMapperBase> mapper,
    ::TIntrusivePtr<IReducerBase> reduceCombiner,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOperationOptions& options)
{
    Y_VERIFY(reducer.Get());

    return DoMapReduce(
        spec,
        mapper.Get(),
        reduceCombiner.Get(),
        *reducer,
        options);
}

IOperationPtr IOperationClient::Sort(
    const TOneOrMany<TRichYPath>& input,
    const TRichYPath& output,
    const TKeyColumns& sortBy,
    const TSortOperationSpec& spec,
    const TOperationOptions& options)
{
    Y_ENSURE_EX(spec.Inputs_.empty(),
        TApiUsageError() << "TSortOperationSpec::Inputs MUST be empty");
    Y_ENSURE_EX(spec.Output_.Path_.empty(),
        TApiUsageError() << "TSortOperationSpec::Output MUST be empty");
    Y_ENSURE_EX(spec.SortBy_.Parts_.empty(),
        TApiUsageError() << "TSortOperationSpec::SortBy MUST be empty");

    auto sortSpec = spec;
    for (const auto& inputPath : input.Parts_)
        sortSpec.AddInput(inputPath);
    sortSpec.Output(output);
    sortSpec.SortBy(sortBy);
    return Sort(sortSpec, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
