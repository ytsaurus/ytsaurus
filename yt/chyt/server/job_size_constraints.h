#pragma once

#include "private.h"

#include <yt/yt/server/lib/controller_agent/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

NControllerAgent::IJobSizeConstraintsPtr CreateClickHouseJobSizeConstraints(
    TSubqueryConfigPtr config,
    i64 totalDataWeight,
    i64 totalRowCount,
    int jobCount,
    std::optional<double> samplingRate,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
