#pragma once
#ifndef CRASH_HANDLER_INL_H_
#error "Direct inclusion of this file is not allowed, include crash_handler-inl.h"
#endif

#include "stack_trace.h"

#include <yt/core/misc/raw_formatter.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_
namespace NDetail {

int GetSymbolInfo(void* pc, char* buffer, int length);

//! Dumps information about the stack frame.
template <class TCallback>
void DumpStackFrameInfo(void* pc, TCallback flushCallback)
{
    TRawFormatter<1024> formatter;

    formatter.AppendString("@ ");
    const int width = (sizeof(void*) == 8 ? 12 : 8) + 2;
    // +2 for "0x"; 12 for x86_64 because higher bits are always zeroed.
    formatter.AppendNumberAsHexWithPadding(reinterpret_cast<uintptr_t>(pc), width);
    formatter.AppendString(" ");
    // Get the symbol from the previous address of PC,
    // because PC may be in the next function.
    formatter.Advance(GetSymbolInfo(
        reinterpret_cast<char*>(pc) - 1,
        formatter.GetCursor(),
        formatter.GetBytesRemaining()));
    formatter.AppendString("\n");

    flushCallback(formatter.GetData(), formatter.GetBytesWritten());
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TCallback>
void DumpStackTrace(TCallback flushCallback)
{
    // Get the stack trace (without current frame hence +1).
    std::array<void*, 99> stack; // 99 is to keep formatting. :)
    const int depth = GetStackTrace(stack.data(), stack.size(), 1);

    TRawFormatter<256> formatter;

    // Dump the stack trace.
    for (int i = 0; i < depth; ++i) {
        formatter.Reset();
        formatter.AppendNumber(i + 1, 10, 2);
        formatter.AppendString(". ");
        flushCallback(formatter.GetData(), formatter.GetBytesWritten());
        NDetail::DumpStackFrameInfo(stack[i], flushCallback);
    }
}

#else

template <class TCallback>
void DumpStackTrace(TCallback flushCallback)
{
    TRawFormatter<256> formatter;
    formatter.AppendString("(stack trace is not available for this platform)");
    flushCallback(formatter.GetData(), formatter.GetBytesWritten());
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
