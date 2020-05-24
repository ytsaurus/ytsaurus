#pragma once

#include <util/generic/string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TCallback>
void FormatStackTrace(const void* const* frames, int frameCount, TCallback writeCallback);

TString FormatStackTrace(const void* const* frames, int frameCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define STACK_TRACE_INL_H_
#include "stack_trace-inl.h"
#undef STACK_TRACE_INL_H_
