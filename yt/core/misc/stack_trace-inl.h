#ifndef STACK_TRACE_INL_H_
#error "Direct inclusion of this file is not allowed, include stack_trace.h"
// For the sake of sane code completion.
#include "stack_trace.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

int GetSymbolInfo(void* pc, char* buffer, int length);
void DumpStackFrameInfo(TRawFormatter<1024>* formatter, void* pc);

} // namespace NDetail

template <class TCallback>
void FormatStackTrace(void** frames, int frameCount, TCallback writeCallback)
{
    TRawFormatter<1024> formatter;

    // Dump the stack trace.
    for (int i = 0; i < frameCount; ++i) {
        formatter.Reset();
        formatter.AppendNumber(i + 1, 10, 2);
        formatter.AppendString(". ");
        NDetail::DumpStackFrameInfo(&formatter, frames[i]);
        writeCallback(formatter.GetData(), formatter.GetBytesWritten());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
