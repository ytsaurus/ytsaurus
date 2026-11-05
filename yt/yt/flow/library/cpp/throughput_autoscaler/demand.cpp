#include "demand.h"

#include <algorithm>
#include <cmath>

namespace NYT::NFlow {
namespace {

std::optional<double> FilterInvalidValue(std::optional<double> value)
{
    return value && std::isfinite(*value) && *value >= 0 ? value : std::nullopt;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TThroughputDemand BuildThroughputDemand(
    const TThroughputSpecPtr& config,
    std::optional<double> offered,
    std::optional<double> ready,
    TDuration actualBatchDuration,
    const TThroughputDemand* upstream)
{
    offered = FilterInvalidValue(offered);
    ready = FilterInvalidValue(ready);
    TThroughputDemand result;
    result.SteadyDemand = upstream ? FilterInvalidValue(upstream->SteadyDemand) : offered;
    if (result.SteadyDemand && ready) {
        result.NormalBacklog = FilterInvalidValue(config->NormalBacklogBatchCount * actualBatchDuration.SecondsFloat() * *result.SteadyDemand);
        if (result.NormalBacklog) {
            result.ExcessBacklog = std::max(0.0, *ready - *result.NormalBacklog);
        }
    }
    if (!result.ExcessBacklog) {
        return result;
    }
    double catchUp = *result.ExcessBacklog / config->CatchUpPeriod.SecondsFloat();
    if (auto incoming = upstream ? FilterInvalidValue(upstream->CatchUpDemand) : offered) {
        result.CatchUpDemand = FilterInvalidValue(*incoming + catchUp);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
