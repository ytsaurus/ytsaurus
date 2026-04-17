#pragma once

#include "profile_manager.h"

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

IQueueExporterProfileManagerPtr CreateQueueExporterProfileManager(
    const NProfiling::TProfiler& profiler,
    const std::string& exportName,
    const NLogging::TLogger& logger,
    const NQueueClient::TQueueTableRow& row,
    bool leading);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
