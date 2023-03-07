#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TCallback>
void FormatStackTrace(void** frames, int frameCount, const TCallback& writeCallback);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define STACK_TRACE_INL_H_
#include "stack_trace-inl.h"
#undef STACK_TRACE_INL_H_
