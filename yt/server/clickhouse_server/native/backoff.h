#pragma once

#include <yt/core/misc/property.h>

#include <util/datetime/base.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

// Returns duration from range [d * (1 - jitter), d * (1 + jitter)]
TDuration AddJitter(const TDuration& d, double jitter);

////////////////////////////////////////////////////////////////////////////////

// TODO: IBackoff

struct TBackoffOptions
{
    DEFINE_BYVAL_RW_PROPERTY(TDuration, InitialPause, TDuration::Seconds(5))
    DEFINE_BYVAL_RW_PROPERTY(double, Factor, 1.5)
    DEFINE_BYVAL_RW_PROPERTY(TDuration, PauseUpperBound, TDuration::Minutes(1))
    DEFINE_BYVAL_RW_PROPERTY(double, Jitter, 0.2)
};

class TBackoff
{
public:
    TBackoff(const TBackoffOptions& options = TBackoffOptions())
        : Options(options)
        , Pause(options.GetInitialPause())
    {}

    TDuration GetNextPause()
    {
        auto pause = AddJitter(Pause, Options.GetJitter());
        Pause = Min(Pause * Options.GetFactor(), Options.GetPauseUpperBound());
        return pause;
    }

    void Reset()
    {
        Pause = Options.GetInitialPause();
    }

    void ResetTo(TDuration pause)
    {
        Pause = pause;
    }

private:
    TBackoffOptions Options;
    TDuration Pause;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
