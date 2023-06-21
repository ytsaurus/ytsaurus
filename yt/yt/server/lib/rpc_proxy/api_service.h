#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/library/profiling/public.h>

#include <yt/yt/library/tracing/jaeger/public.h>

#include <yt/yt/core/rpc/service.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct IApiService
    : public virtual NRpc::IService
{
    //! Thread affinity: any.
    virtual void OnDynamicConfigChanged(const TApiServiceDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IApiService)

////////////////////////////////////////////////////////////////////////////////

//! Custom #stickyTransactionPool is useful for sharing transactions
//! between services (e.g.: ORM and RPC proxy).
IApiServicePtr CreateApiService(
    TApiServiceConfigPtr config,
    IInvokerPtr workerInvoker,
    NApi::NNative::IConnectionPtr connection,
    NRpc::IAuthenticatorPtr authenticator,
    IProxyCoordinatorPtr proxyCoordinator,
    IAccessCheckerPtr accessChecker,
    ISecurityManagerPtr securityManager,
    NTracing::TSamplerPtr traceSampler,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    NApi::IStickyTransactionPoolPtr stickyTransactionPool = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
