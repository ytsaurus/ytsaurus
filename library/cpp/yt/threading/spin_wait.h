#pragma once

#include <library/cpp/yt/cpu_clock/clock.h>

#include <util/datetime/base.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

class TSpinWait
{
public:
    ~TSpinWait();

    void Wait();

private:
    int SpinIteration_ = 0;
    int SleepIteration_ = 0;

    TCpuInstant SlowPathStartInstant_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
