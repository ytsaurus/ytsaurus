#pragma once
#ifndef THROTTLE_INL_H_
#error "Direct inclusion of this file is not allowed, include throttle.h"
// For the sake of sane code completion.
#include "throttle.h"
#endif

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

NRoren::TTransform<ui64, void> ReportThrottlerProcessedImpl(ui64 flushSize);

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <typename T>
NRoren::TTransform<T, T> ReportThrottlerProcessed(ui64 (*sizeFn)(const T&), ui64 flushSize)
{
    return [sizeFn, flushSize] (const NRoren::TPCollection<T>& rows) -> NRoren::TPCollection<T> {
        rows
            | NRoren::ParDo(sizeFn)
            | NPrivate::ReportThrottlerProcessedImpl(flushSize);
        return rows;
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
