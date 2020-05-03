#pragma once

#include <yt/server/lib/job_satellite_connection/public.h>

#include <util/generic/string.h>

namespace NYT::NExec {

////////////////////////////////////////////////////////////////////////////////

void RunJobSatellite(
    NJobSatelliteConnection::TJobSatelliteConnectionConfigPtr config,
    int uid,
    const TString& jobId);

void NotifyExecutorPrepared(NJobSatelliteConnection::TJobSatelliteConnectionConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExec
