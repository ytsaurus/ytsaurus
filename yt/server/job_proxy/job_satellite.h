#pragma once

#include "public.h"
#include "job_satellite_connection.h"

#include <util/generic/stroka.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

void RunJobSatellite(
    TJobSatelliteConnectionConfigPtr config,
    int uid,
    const std::vector<Stroka>& env,
    const Stroka& jobId);
void NotifyExecutorPrepared(TJobSatelliteConnectionConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
