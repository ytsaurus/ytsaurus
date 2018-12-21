#pragma once

#include <yt/core/bus/public.h>

#include <yt/core/misc/public.h>

#include <yt/core/yson/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT::NJobProberClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IJobProbe)

DEFINE_ENUM(EErrorCode,
    ((JobIsNotRunning) (17000))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProberClient
