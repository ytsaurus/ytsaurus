#include "epoch_cycle_tracker.h"

#include <algorithm>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TEpochCycleTracker::SetWindow(size_t window)
{
    auto guard = Guard(Lock_);
    Window_ = std::clamp<size_t>(window, 1, MaxWindowSize);
}

void TEpochCycleTracker::RecordCycle(TDuration cycleDuration)
{
    auto guard = Guard(Lock_);
    Samples_[NextIndex_] = cycleDuration;
    NextIndex_ = (NextIndex_ + 1) % MaxWindowSize;
    SampleCount_ = std::min(SampleCount_ + 1, MaxWindowSize);
}

std::optional<TDuration> TEpochCycleTracker::GetMedianCycle() const
{
    std::array<TDuration, MaxWindowSize> samples;
    size_t count = 0;
    {
        auto guard = Guard(Lock_);
        if (SampleCount_ == 0) {
            return std::nullopt;
        }
        count = std::min(SampleCount_, Window_);
        for (size_t i = 0; i < count; ++i) {
            samples[i] = Samples_[(NextIndex_ + MaxWindowSize - 1 - i) % MaxWindowSize];
        }
    }
    auto middle = samples.begin() + count / 2;
    std::nth_element(samples.begin(), middle, samples.begin() + count);
    return *middle;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
