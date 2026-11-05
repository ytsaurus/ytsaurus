#pragma once

#include <yt/yt/core/logging/log.h>

#include <optional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Starts the porto resource tracker if the node runs in a porto environment.
//! Does nothing in builds without porto support.
void TryEnablePortoResourceTracker(std::optional<double> vcpuFactor, const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
