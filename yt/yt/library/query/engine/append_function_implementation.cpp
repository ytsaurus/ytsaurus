#include "functions_cg.h"

#include <yt/yt/library/query/engine_api/append_function_implementation.h>
#include <yt/yt/library/query/engine_api/builtin_function_profiler.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void AppendFunctionImplementation(
    const TFunctionProfilerMapPtr& functionProfilers,
    const TAggregateProfilerMapPtr& aggregateProfilers,
    bool functionIsAggregate,
    const std::string& functionName,
    const std::string& functionSymbolName,
    ECallingConvention functionCallingConvention,
    TSharedRef functionChunkSpecsFingerprint,
    TType functionRepeatedArgType,
    int functionRepeatedArgIndex,
    bool functionUseFunctionContext,
    const TSharedRef& functionImpl)
{
    YT_VERIFY(!functionImpl.Empty());

    if (functionIsAggregate) {
        aggregateProfilers->emplace(functionName, New<TExternalAggregateCodegen>(
            functionName,
            functionImpl,
            functionCallingConvention,
            /*isFirst*/ false,
            functionChunkSpecsFingerprint));
    } else {
        functionProfilers->emplace(functionName, New<TExternalFunctionCodegen>(
            functionName,
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
