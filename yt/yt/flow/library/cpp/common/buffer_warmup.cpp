#include "buffer_warmup.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TPartitionBufferWarmup::Register(TRegistrar registrar)
{
    registrar.Parameter("input_speeds", &TThis::InputSpeeds)
        .Default();
    registrar.Parameter("output_speeds", &TThis::OutputSpeeds)
        .Default();
    registrar.Parameter("epoch_cycle_seconds", &TThis::EpochCycleSeconds)
        .Default(0.0);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

// Matching the v2_publish_threshold default is a coincidence: this gates state
// rewrites, that one gates limit publications; tune them independently.
constexpr double DriftThreshold = 0.25;

bool WarmupSpeedsDiffer(const THashMap<TStreamId, double>& oldSpeeds, const THashMap<TStreamId, double>& newSpeeds)
{
    auto differ = [] (double oldValue, double newValue) {
        return std::abs(newValue - oldValue) > DriftThreshold * std::max({oldValue, newValue, 1.0});
    };
    for (const auto& [streamId, newValue] : newSpeeds) {
        if (differ(GetOrDefault(oldSpeeds, streamId, 0.0), newValue)) {
            return true;
        }
    }
    for (const auto& [streamId, oldValue] : oldSpeeds) {
        if (!newSpeeds.contains(streamId) && differ(oldValue, 0.0)) {
            return true;
        }
    }
    return false;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool WarmupDiffers(const TPartitionBufferWarmup& oldWarmup, const TPartitionBufferWarmup& newWarmup)
{
    return WarmupSpeedsDiffer(oldWarmup.InputSpeeds, newWarmup.InputSpeeds) ||
        WarmupSpeedsDiffer(oldWarmup.OutputSpeeds, newWarmup.OutputSpeeds) ||
        std::abs(newWarmup.EpochCycleSeconds - oldWarmup.EpochCycleSeconds) >
        DriftThreshold * std::max({oldWarmup.EpochCycleSeconds, newWarmup.EpochCycleSeconds, 1.0});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
