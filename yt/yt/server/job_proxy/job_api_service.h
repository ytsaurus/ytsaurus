#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateJobApiService(
    TJobApiServiceConfigPtr config,
    IInvokerPtr controlInvoker,
    TWeakPtr<TJobProxy> jobProxy);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
