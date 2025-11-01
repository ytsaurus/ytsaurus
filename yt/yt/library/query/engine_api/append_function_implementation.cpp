#include "append_function_implementation.h"

#include <yt/yt/library/query/base/query_helpers.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void AppendFunctionImplementation(
    const TFunctionProfilerMapPtr& /*functionProfilers*/,
    const TAggregateProfilerMapPtr& /*aggregateProfilers*/,
    bool /*functionIsAggregate*/,
    const std::string& /*functionName*/,
    const std::string& /*functionSymbolName*/,
    ECallingConvention /*functionCallingConvention*/,
    TSharedRef /*functionChunkSpecsFingerprint*/,
    TType /*functionRepeatedArgType*/,
    int /*functionRepeatedArgIndex*/,
    bool /*functionUseFunctionContext*/,
    const TEnumIndexedArray<NCodegen::EExecutionBackend, TSharedRef>& /*implementationFiles*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/append_function_implementation.cpp
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
