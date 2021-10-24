#ifndef TSCP_INL_H_
#error "Direct inclusion of this file is not allowed, include tscp.h"
// For the sake of sane code completion.
#include "tscp.h"
#endif
#undef TSCP_INL_H_

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

inline TTscp TTscp::Get()
{
    ui64 rax, rcx, rdx;
    asm volatile ( "rdtscp\n" : "=a" (rax), "=c" (rcx), "=d" (rdx) : : );
    return TTscp{
        .Instant = static_cast<TCpuInstant>((rdx << 32) + rax),
        .ProcessorId = static_cast<int>(rcx) & (MaxProcessorId - 1)
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
