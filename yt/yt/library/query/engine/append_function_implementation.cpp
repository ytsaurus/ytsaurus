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
    const TEnumIndexedArray<NCodegen::EExecutionBackend, TSharedRef>& implementationFiles)
{
    if (functionIsAggregate) {
        aggregateProfilers->emplace(functionName, New<TExternalAggregateCodegen>(
            functionName,
            implementationFiles,
            functionRepeatedArgIndex,
            functionRepeatedArgType,
            /*isFirst*/ false,
            functionChunkSpecsFingerprint));
    } else {
        functionProfilers->emplace(functionName, New<TExternalFunctionCodegen>(
            functionName,
            functionSymbolName,
            implementationFiles,
            functionCallingConvention,
            functionRepeatedArgType,
            functionRepeatedArgIndex,
            functionUseFunctionContext,
            functionChunkSpecsFingerprint));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
