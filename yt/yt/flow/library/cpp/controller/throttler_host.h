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
//! Owns the service and translates the dynamic pipeline spec into the
//! service's config, reconfiguring it on every spec change.
struct IThrottlerHost
    : public TRefCounted
{
    //! Reconfigure the throttler service from the dynamic pipeline spec.
    //! Throttler names and their token-bucket configs come from
    //! `dynamicSpec->Throttlers`; throttlers that vanish from the spec are
    //! torn down, new ones are added, existing ones are reconfigured in place.
    virtual void Reconfigure(const TDynamicPipelineSpecPtr& dynamicSpec) = 0;

    //! Returns the underlying RPC service for registration on an RPC server.
    virtual NRpc::IServicePtr GetRpcService() = 0;
};

DEFINE_REFCOUNTED_TYPE(IThrottlerHost);

IThrottlerHostPtr CreateThrottlerHost(
    IInvokerPtr invoker,
    NYPath::TRichYPath pipelinePath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
