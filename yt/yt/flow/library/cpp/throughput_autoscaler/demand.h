#pragma once

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <optional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TThroughputDemand
{
    std::optional<double> SteadyDemand;
    std::optional<double> CatchUpDemand;
    std::optional<double> NormalBacklog;
    std::optional<double> ExcessBacklog;
};

//! Converts local ready work and an optional upstream forecast to demand in one work unit.
TThroughputDemand BuildThroughputDemand(
    const TThroughputSpecPtr& config,
    std::optional<double> offered,
    std::optional<double> ready,
    TDuration actualBatchDuration,
    const TThroughputDemand* upstream = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
