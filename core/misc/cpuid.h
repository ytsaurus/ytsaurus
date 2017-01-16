#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCpuId
{
public:
    TCpuId();

#define X(name, r, bit) bool name() const { return (r) & (1U << bit); }

// cpuid(1): Processor Info and Feature Bits.
#define C(name, bit) X(name, F1c_, bit)
    C(Sse3, 0)
    C(Pclmuldq, 1)
    C(Dtes64, 2)
    C(Monitor, 3)
    C(Dscpl, 4)
    C(Vmx, 5)
    C(Smx, 6)
    C(Eist, 7)
    C(Tm2, 8)
    C(Ssse3, 9)
    C(Cnxtid, 10)
    C(Fma, 12)
    C(Cx16, 13)
    C(Xtpr, 14)
    C(Pdcm, 15)
    C(Pcid, 17)
    C(Dca, 18)
    C(Sse41, 19)
    C(Sse42, 20)
    C(X2apic, 21)
    C(Movbe, 22)
    C(Popcnt, 23)
    C(Tscdeadline, 24)
    C(Aes, 25)
    C(Xsave, 26)
    C(Osxsave, 27)
    C(Avx, 28)
    C(F16c, 29)
    C(Rdrand, 30)
#undef C

#define D(name, bit) X(name, F1d_, bit)
    D(Fpu, 0)
    D(Vme, 1)
    D(De, 2)
    D(Pse, 3)
    D(Tsc, 4)
    D(Msr, 5)
    D(Pae, 6)
    D(Mce, 7)
    D(Cx8, 8)
    D(Apic, 9)
    D(Sep, 11)
    D(Mtrr, 12)
    D(Pge, 13)
    D(Mca, 14)
    D(Cmov, 15)
    D(Pat, 16)
    D(Pse36, 17)
    D(Psn, 18)
    D(Clfsh, 19)
    D(Ds, 21)
    D(Acpi, 22)
    D(Mmx, 23)
    D(Fxsr, 24)
    D(Sse, 25)
    D(Sse2, 26)
    D(Ss, 27)
    D(Htt, 28)
    D(Tm, 29)
    D(Pbe, 31)
#undef D
// cpuid(7): Extended Features.
#define B(name, bit) X(name, F7b_, bit)
    B(Bmi1, 3)
    B(Hle, 4)
    B(Avx2, 5)
    B(Smep, 7)
    B(Bmi2, 8)
    B(Erms, 9)
    B(Invpcid, 10)
    B(Rtm, 11)
    B(Mpx, 14)
    B(Avx512f, 16)
    B(Avx512dq, 17)
    B(Rdseed, 18)
    B(Adx, 19)
    B(Smap, 20)
    B(Avx512ifma, 21)
    B(Pcommit, 22)
    B(Clflushopt, 23)
    B(Clwb, 24)
    B(Avx512pf, 26)
    B(Avx512er, 27)
    B(Avx512cd, 28)
    B(Sha, 29)
    B(Avx512bw, 30)
    B(Avx512vl, 31)
#undef B

#define C(name, bit) X(name, F7c_, bit)
    C(Prefetchwt1, 0)
    C(Avx512vbmi, 1)
#undef C

#undef X

private:
    uint32_t F1c_ = 0;
    uint32_t F1d_ = 0;
    uint32_t F7b_ = 0;
    uint32_t F7c_ = 0;
};

extern const TCpuId CpuId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
