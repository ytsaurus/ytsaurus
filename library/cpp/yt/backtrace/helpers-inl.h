#pragma once
#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include <util/system/compiler.h>

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

template <class TCursor>
Y_NO_INLINE TRange<const void*> GetBacktraceFromCursor(
    TCursor* cursor,
    TMutableRange<const void*> frameBuffer,
    int framesToSkip)
{
    // Account for the current frame.
    ++framesToSkip;
    size_t frameCount = 0;
    while (frameCount < frameBuffer.size() && !cursor->IsFinished()) {
        if (framesToSkip > 0) {
            --framesToSkip;
        } else {
            frameBuffer[frameCount++] = cursor->GetCurrentIP();
        }
        cursor->MoveNext();
    }
    return {frameBuffer.begin(), frameCount};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
