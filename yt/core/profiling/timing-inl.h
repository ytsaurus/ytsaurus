#pragma once
#ifndef TIMING_INL_H_
#error "Direct inclusion of this file is not allowed, include timing.h"
// For the sake of sane code completion
#include "timing.h"
#endif
#undef TIMING_INL_H_

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TCpuInstant GetCpuInstant()
{
    // See datetime.h
    unsigned hi, lo;
    __asm__ __volatile__("rdtsc"
                         : "=a"(lo), "=d"(hi));
    return static_cast<unsigned long long>(lo) | (static_cast<unsigned long long>(hi) << 32);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
