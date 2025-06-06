#pragma once

#include <library/cpp/yt/cpu_clock/clock.h>

#include <library/cpp/yt/misc/enum.h>

#include <util/system/types.h>

#include <util/generic/size_literals.h>

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EStockpileStrategy,
    ((FixedBreaks)          (0))
    ((FlooredLoad)          (1))
    ((ProgressiveBackoff)   (2))
);

////////////////////////////////////////////////////////////////////////////////

struct TStockpileOptions
{
    static constexpr i64 DefaultBufferSize = 4_GBs;
    i64 BufferSize = DefaultBufferSize;

    static constexpr int DefaultThreadCount = 1;
    int ThreadCount = DefaultThreadCount;

    static constexpr EStockpileStrategy DefaultStrategy = EStockpileStrategy::FlooredLoad;
    EStockpileStrategy Strategy = DefaultStrategy;

    static constexpr TDuration PeriodQuantum = TDuration::MilliSeconds(10);
    static constexpr TDuration DefaultPeriod = TDuration::MilliSeconds(10);
    TDuration Period = DefaultPeriod;
};

////////////////////////////////////////////////////////////////////////////////

class TStockpileManager
{
public:
    //! Configures the background stockpile threads.
    //! Safe to call multiple times.
    static void Reconfigure(TStockpileOptions options);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
