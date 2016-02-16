#pragma once

#include "common.h"

namespace NYT {
namespace NHRTimer {

////////////////////////////////////////////////////////////////////////////////

// Returns CPU internal cycle counter.
// On modern systems, cycle counters are consistent across cores and cycle rate
// can be considered constant for practical purposes.
Y_FORCE_INLINE ui64 GetRdtsc()
{
#ifdef _win_
    return __rdtsc();
#else
    #if defined(_x86_64_)
        unsigned hi, lo;
        __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
        return ((ui64)lo) | (((ui64)hi) << 32);
    #elif defined(_i386_)
        ui64 r;
        __asm__ __volatile__ ("rdtsc" : "=A" (r));
        return r;
    #else
        #error "Unsupported architecture"
    #endif
#endif
}

// Represents an offset from an arbitrary point in the past;
// it should be used only for relative measurements.
struct THRInstant
{
    long Seconds;
    long Nanoseconds;
};

// Represents a duration in nano-seconds.
typedef ui64 THRDuration;

#ifdef _linux_
static_assert(
    sizeof(THRInstant) == sizeof(struct timespec),
    "THRInstant should be ABI-compatible with struct timespec");
static_assert(
    offsetof(THRInstant, Seconds) == offsetof(struct timespec, tv_sec),
    "THRInstant should be ABI-compatible with struct timespec");
static_assert(
    offsetof(THRInstant, Nanoseconds) == offsetof(struct timespec, tv_nsec),
    "THRInstant should be ABI-compatible with struct timespec");
#endif

// Returns instant.
void GetHRInstant(THRInstant* instant);

// Returns time difference in nanoseconds.
THRDuration GetHRDuration(const THRInstant& begin, const THRInstant& end);

// Returns instant resolution.
THRDuration GetHRResolution();

////////////////////////////////////////////////////////////////////////////////

} // namespace NHRTimer
} // namespace NYT
