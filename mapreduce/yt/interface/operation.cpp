#include "operation.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
    i64 OutputTableCount = -1;
} // namespace NDetail

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

TUserJobSpec& TUserJobSpec::JobBinaryLocalPath(TString path)
{
    JobBinary_ = TJobBinaryLocalPath{path};
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

IOperationPtr IOperationClient::Map(
    const TMapOperationSpec& spec,
    ::TIntrusivePtr<IMapperBase> mapper,
    const TOperationOptions& options)
{
    Y_VERIFY(mapper.Get());

    mapper->CheckInputFormat("mapper", spec.GetInputDesc());
    mapper->CheckOutputFormat("mapper", spec.GetOutputDesc());

    return DoMap(
        spec,
        *mapper,
        options);
}

IOperationPtr IOperationClient::Reduce(
    const TReduceOperationSpec& spec,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOperationOptions& options)
{
    Y_VERIFY(reducer.Get());

    reducer->CheckInputFormat("reducer", spec.GetInputDesc());
    reducer->CheckOutputFormat("reducer", spec.GetOutputDesc());

    return DoReduce(
        spec,
        *reducer,
        options);
}

IOperationPtr IOperationClient::JoinReduce(
    const TJoinReduceOperationSpec& spec,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOperationOptions& options)
{
    Y_VERIFY(reducer.Get());

    reducer->CheckInputFormat("reducer", spec.GetInputDesc());
    reducer->CheckOutputFormat("reducer", spec.GetOutputDesc());

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

    if (mapper) {
        mapper->CheckInputFormat("mapper", spec.GetInputDesc());
    } else {
        reducer->CheckInputFormat("reducer", spec.GetInputDesc());
    }
    reducer->CheckOutputFormat("reducer", spec.GetOutputDesc());

    TMultiFormatDesc dummy, outputMapperDesc, inputReducerDesc;
    if (mapper) {
        mapper->AddOutputFormatDescription(&outputMapperDesc);
    }
    reducer->AddInputFormatDescription(&inputReducerDesc);

    return DoMapReduce(
        spec,
        mapper.Get(),
        nullptr,
        *reducer,
        outputMapperDesc,
        dummy,
        dummy,
        inputReducerDesc,
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

    if (mapper) {
        mapper->CheckInputFormat("mapper", spec.GetInputDesc());
    } else {
        reducer->CheckInputFormat("reducer", spec.GetInputDesc());
    }
    reducer->CheckOutputFormat("reducer", spec.GetOutputDesc());

    TMultiFormatDesc outputMapperDesc, inputReducerDesc,
        inputReduceCombinerDesc, outputReduceCombinerDesc;
    if (mapper) {
        mapper->AddOutputFormatDescription(&outputMapperDesc);
    }
    reducer->AddInputFormatDescription(&inputReducerDesc);
    if (reduceCombiner) {
        reduceCombiner->AddInputFormatDescription(&inputReduceCombinerDesc);
        reduceCombiner->AddOutputFormatDescription(&outputReduceCombinerDesc);
    }

    return DoMapReduce(
        spec,
        mapper.Get(),
        reduceCombiner.Get(),
        *reducer,
        outputMapperDesc,
        inputReduceCombinerDesc,
        outputReduceCombinerDesc,
        inputReducerDesc,
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
