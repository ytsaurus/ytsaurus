#pragma once

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/job_tracker_client/public.h>

namespace NYT::NJobProberClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IJobProbe)
DECLARE_REFCOUNTED_CLASS(TJobShellDescriptorCache)

DEFINE_ENUM(EErrorCode,
    ((JobIsNotRunning) (17000))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProberClient
