#pragma once

#ifndef PROCESS_FUNCTION_GRAPH_TEST_HARNESS_INL_H_
    #error "Direct inclusion of this file is not allowed, include process_function_graph_test_harness.h"
    // For the sake of sane code completion.
    #include "process_function_graph_test_harness.h"
#endif

#include <util/system/type_name.h>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

template <CYsonMessage T>
TProcessFunctionGraphTestHarness& TProcessFunctionGraphTestHarness::RegisterStream(const TStreamId& streamId)
{
    auto spec = New<TStreamSpec>();
    spec->ClassName = TypeName<T>();
    spec->Schema = GetYsonMessagePayloadSchema<T>();
    return RegisterStream(streamId, std::move(spec));
}

template <class TState>
TState TProcessFunctionGraphTestHarness::ReadNodeState(TStringBuf nodeId, TStringBuf name, const TKey& key)
{
    return Environment_.ReadKeyState<TState>(Format("functions/%v/%v", nodeId, name), key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
