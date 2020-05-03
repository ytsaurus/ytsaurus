#pragma once

#include <yt/server/lib/job_proxy/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TUserJobWriteController;

DECLARE_REFCOUNTED_STRUCT(IJob)
DECLARE_REFCOUNTED_STRUCT(IJobHost)

DECLARE_REFCOUNTED_CLASS(TJobProxy)
DECLARE_REFCOUNTED_CLASS(TCpuMonitor)

DECLARE_REFCOUNTED_STRUCT(IResourceTracker)
DECLARE_REFCOUNTED_STRUCT(IJobProxyEnvironment)
DECLARE_REFCOUNTED_STRUCT(IUserJobEnvironment)

DECLARE_REFCOUNTED_CLASS(TGpuCoreReader)
DECLARE_REFCOUNTED_CLASS(TCoreWatcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
