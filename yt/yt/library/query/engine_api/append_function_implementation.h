#pragma once

#include "public.h"

#include <yt/yt/library/codegen_api/execution_backend.h>

#include <yt/yt/library/query/base/functions_common.h>

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
    const TEnumIndexedArray<NCodegen::EExecutionBackend, TSharedRef>& implementationFiles);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
