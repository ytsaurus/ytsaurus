#pragma once

#include <library/cpp/yt/memory/range.h>

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

//! Obtains a backtrace via a given cursor.
/*!
 *  \param frameBuffer is the buffer where the backtrace is written to
 *  \param framesToSkip is the number of top frames to skip
 *  \returns the portion of #frameBuffer that has actually been filled
 */
template <class TCursor>
TRange<const void*> GetBacktraceFromCursor(
    TCursor* cursor,
    TMutableRange<const void*> frameBuffer,
    int framesToSkip);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
