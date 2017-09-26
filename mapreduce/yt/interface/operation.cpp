#include "operation.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

IOperationPtr IOperationClient::Map(
    const TMapOperationSpec& spec,
    ::TIntrusivePtr<IMapperBase> mapper,
    const TOperationOptions& options)
{
    Y_VERIFY(mapper.Get());

    mapper->CheckInputFormat("mapper", spec.InputDesc_);
    mapper->CheckOutputFormat("mapper", spec.OutputDesc_);

    return DoMap(
        spec,
        mapper.Get(),
        options);
}

IOperationPtr IOperationClient::Reduce(
    const TReduceOperationSpec& spec,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOperationOptions& options)
{
    Y_VERIFY(reducer.Get());

    reducer->CheckInputFormat("reducer", spec.InputDesc_);
    reducer->CheckOutputFormat("reducer", spec.OutputDesc_);

    return DoReduce(
        spec,
        reducer.Get(),
        options);
}

IOperationPtr IOperationClient::JoinReduce(
    const TJoinReduceOperationSpec& spec,
    ::TIntrusivePtr<IReducerBase> reducer,
    const TOperationOptions& options)
{
    Y_VERIFY(reducer.Get());

    reducer->CheckInputFormat("reducer", spec.InputDesc_);
    reducer->CheckOutputFormat("reducer", spec.OutputDesc_);

    return DoJoinReduce(
        spec,
        reducer.Get(),
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
        mapper->CheckInputFormat("mapper", spec.InputDesc_);
    } else {
        reducer->CheckInputFormat("reducer", spec.InputDesc_);
    }
    reducer->CheckOutputFormat("reducer", spec.OutputDesc_);

    TMultiFormatDesc dummy, outputMapperDesc, inputReducerDesc;
    if (mapper) {
        mapper->AddOutputFormatDescription(&outputMapperDesc);
    }
    reducer->AddInputFormatDescription(&inputReducerDesc);

    return DoMapReduce(
        spec,
        mapper.Get(),
        nullptr,
        reducer.Get(),
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
        mapper->CheckInputFormat("mapper", spec.InputDesc_);
    } else {
        reducer->CheckInputFormat("reducer", spec.InputDesc_);
    }
    reducer->CheckOutputFormat("reducer", spec.OutputDesc_);

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
        reducer.Get(),
        outputMapperDesc,
        inputReduceCombinerDesc,
        outputReduceCombinerDesc,
        inputReducerDesc,
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
