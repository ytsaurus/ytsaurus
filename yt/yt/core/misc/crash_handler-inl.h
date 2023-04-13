#ifndef CRASH_HANDLER_INL_H_
#error "Direct inclusion of this file is not allowed, include crash_handler-inl.h"
// For the sake of sane code completion.
#include "crash_handler-inl.h"
#endif

#include "stack_trace.h"

#include <library/cpp/yt/memory/range.h>

#include <array>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

using TStackTrace = TRange<const void*>;
using TStackTraceBuffer = std::array<const void*, 99>; // 99 is to keep formatting :)
TStackTrace GetStackTrace(TStackTraceBuffer* buffer);

} // namespace NDetail

template <class TCallback>
Y_NO_INLINE void DumpStackTrace(TCallback writeCallback)
{
    NDetail::TStackTraceBuffer buffer;
    auto frames = NDetail::GetStackTrace(&buffer);
    if (frames.empty()) {
        writeCallback(TStringBuf("<stack trace is not available>"));
    } else {
        FormatStackTrace(frames.begin(), frames.size(), writeCallback);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
