#pragma once

#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/rpc/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobProberService(
    IJobProbePtr jobProbe,
    IInvokerPtr controlInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
