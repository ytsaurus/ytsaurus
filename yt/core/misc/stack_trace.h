#pragma once

#include "raw_formatter.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

int GetStackTrace(void** frames, int maxFrames, int skipFrames);

template <class TCallback>
void FormatStackTrace(void** frames, int frameCount, TCallback writeCallback);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define STACK_TRACE_INL_H_
#include "stack_trace-inl.h"
#undef STACK_TRACE_INL_H_
