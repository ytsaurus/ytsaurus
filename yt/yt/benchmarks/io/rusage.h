#pragma once

#include "iotest.h"
#include "pstat.h"

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

struct TRusage
{
    TDuration UserTime;
    TDuration SystemTime;

    TRusage operator+=(const TRusage& other);
};

TRusage operator+(const TRusage& lhs, const TRusage& rhs);
TRusage operator-(const TRusage& lhs, const TRusage& rhs);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERusageWho,
    (Process)
    (Thread)
);

TRusage GetRusage(ERusageWho who);

TRusage GetProcessRusage(TProcessId processId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
