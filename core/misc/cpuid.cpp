#include "cpuid.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TCpuId::TCpuId()
{
#ifdef _win_
int reg[4];
__cpuid(static_cast<int*>(reg), 0);
const int n = reg[0];
if (n >= 1) {
    __cpuid(static_cast<int*>(reg), 1);
    F1c_ = reg[2];
    F1d_ = reg[3];
}
if (n >= 7) {
    __cpuidex(static_cast<int*>(reg), 7, 0);
    F7b_ = reg[1];
    F7c_ = reg[2];
}
#else
uint32_t n;
__asm__("cpuid" : "=a"(n) : "a"(0) : "ebx", "edx", "ecx");
if (n >= 1) {
    __asm__("cpuid" : "=c"(F1c_), "=d"(F1d_) : "a"(1) : "ebx");
}
if (n >= 7) {
    __asm__("cpuid" : "=b"(F7b_), "=c"(F7c_) : "a"(7), "c"(0) : "edx");
}
#endif    
}

const TCpuId CpuId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
