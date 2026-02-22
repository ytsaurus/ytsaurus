#pragma once

#include "public.h"

#include <library/cpp/yt/memory/range.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

using TCapturedBacktrace = std::vector<const void*>;

//! Captures the current backtrace.
TCapturedBacktrace CaptureBacktrace();

//! Captures the current backtrace and symbolizes it passing result into #writeCallback.
void DumpBacktrace(
    const std::function<void(TStringBuf)>& writeCallback,
    void* startPC = nullptr);

//! Captures the current backtrace and symbolizes it into a string.
std::string DumpBacktrace();

//! Disables backtrace symbolization. On each DumpBacktrace call,
//! user will receive a message that symbolizing is disabled.
void DisableBacktraceSymbolizing();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
