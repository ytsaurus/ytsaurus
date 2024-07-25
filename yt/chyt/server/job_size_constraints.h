#pragma once

#include "private.h"
#include "query_analyzer.h"

#include <yt/yt/server/lib/chunk_pools/job_size_tracker.h>

#include <yt/yt/server/lib/controller_agent/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TClickHouseJobSizeSpec
{
    NControllerAgent::IJobSizeConstraintsPtr JobSizeConstraints;
    NChunkPools::TJobSizeTrackerOptions JobSizeTrackerOptions;
};

TClickHouseJobSizeSpec CreateClickHouseJobSizeSpec(
    const TExecutionSettingsPtr& executionSettings,
    const TSubqueryConfigPtr& subqueryConfig,
    i64 totalDataWeight,
    i64 totalRowCount,
    int jobCount,
    std::optional<double> samplingRate,
    EReadInOrderMode readInOrderMode,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
