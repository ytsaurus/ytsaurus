#include "functions_cg.h"

#include <yt/yt/library/query/engine_api/append_function_implementation.h>
#include <yt/yt/library/query/engine_api/builtin_function_profiler.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void AppendFunctionImplementation(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers,
    bool functionIsAggregate,
    const TString& functionName,
    const TString& functionSymbolName,
    ECallingConvention functionCallingConvention,
    TSharedRef functionChunkSpecsFingerprint,
    TType functionRepeatedArgType,
    int functionRepeatedArgIndex,
    bool functionUseFunctionContext,
    const TSharedRef& functionImpl)
{
    YT_VERIFY(!functionImpl.Empty());

    const auto& name = functionName;

    if (functionIsAggregate) {
        aggregateProfilers->emplace(name, New<TExternalAggregateCodegen>(
            name,
            functionImpl,
            functionCallingConvention,
            /*isFirst=*/false,
            functionChunkSpecsFingerprint));
    } else {
        functionProfilers->emplace(name, New<TExternalFunctionCodegen>(
            name,
            functionSymbolName,
            functionImpl,
            functionCallingConvention,
            functionRepeatedArgType,
            functionRepeatedArgIndex,
            functionUseFunctionContext,
            functionChunkSpecsFingerprint));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
