#pragma once

#include "public.h"
#include "job_satellite_connection.h"

#include <util/generic/string.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

void RunJobSatellite(
    TJobSatelliteConnectionConfigPtr config,
    int uid,
    const std::vector<TString>& env,
    const TString& jobId);
void NotifyExecutorPrepared(TJobSatelliteConnectionConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
