#pragma once

#include <util/generic/string.h>

#include <functional>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Format given stacktrace, call the `callback` exactly frameCount times.
void FormatStackTrace(const void* const* frames, int frameCount, std::function<void(TStringBuf)> callback);

TString FormatStackTrace(const void* const* frames, int frameCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
