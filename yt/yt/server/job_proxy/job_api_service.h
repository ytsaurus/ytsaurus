#pragma once

#include <yt/yt/server/lib/job_proxy/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobApiService(
    NJobProxy::TJobApiServiceConfigPtr config,
    IInvokerPtr controlInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
