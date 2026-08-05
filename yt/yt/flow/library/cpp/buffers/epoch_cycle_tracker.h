#pragma once

#include "public.h"

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/datetime/base.h>

#include <array>
#include <optional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Tracks the duration of a job's input drain cycle: the time between two
//! consecutive non-empty input batch extractions (an idle wait for the next
//! batch is included — the estimate only raises a floor, boxed by the used peak
//! and max_duration, so its precision does not matter). The window is count-based
//! (last #WindowSize samples), so it adapts to any epoch duration without
//! wall-clock configuration.
class TEpochCycleTracker
    : public TRefCounted
{
public:
    static constexpr size_t MaxWindowSize = 256;
    static constexpr size_t DefaultWindowSize = 16;

    //! Clamped to [1, #MaxWindowSize]; narrows or widens the median window
    //! without dropping the recorded samples.
    void SetWindow(size_t window);

    void RecordCycle(TDuration cycleDuration);

    //! Median over the last #SetWindow samples; nullopt until the first sample.
    std::optional<TDuration> GetMedianCycle() const;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::array<TDuration, MaxWindowSize> Samples_;
    size_t SampleCount_ = 0;
    size_t NextIndex_ = 0;
    size_t Window_ = DefaultWindowSize;
};

DEFINE_REFCOUNTED_TYPE(TEpochCycleTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
