#pragma once

#include "public.h"

#include <yt/ytlib/job_prober_client/public.h>

#include <yt/core/actions/public.h>
#include <yt/core/concurrency/public.h>
#include <yt/core/rpc/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobProberService(
    TJobProxyPtr jobProxy);

NRpc::IServicePtr CreateJobProberService(
    NJobProberClient::IJobProbePtr jobProbe,
    IInvokerPtr controlInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
