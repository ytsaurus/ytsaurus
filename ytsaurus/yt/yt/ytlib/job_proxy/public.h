#pragma once

#include <yt/yt/client/job_proxy/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IJobSpecHelper)
DECLARE_REFCOUNTED_STRUCT(IUserJobIOFactory)
DECLARE_REFCOUNTED_STRUCT(IUserJobReadController)
DECLARE_REFCOUNTED_CLASS(TJobTestingOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
