#pragma once

#include <yt/yt/ytlib/job_proxy/config.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> GetBreakpointEvent(
    const TEventsOnFsConfigPtr& config,
    NJobTrackerClient::TJobId jobId,
    EBreakpointType breakpoint);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
