#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

//! Hosts the distributed throttler RPC service on the controller.
struct IThrottlerHost
    : public TRefCounted
{
    virtual void Reconfigure(const TDynamicPipelineSpecPtr& spec) = 0;

    //! Returns the throttler RPC service.
    virtual NRpc::IServicePtr GetRpcService() = 0;
};

DEFINE_REFCOUNTED_TYPE(IThrottlerHost);

IThrottlerHostPtr CreateThrottlerHost(
    IInvokerPtr invoker,
    NYPath::TRichYPath pipelinePath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
