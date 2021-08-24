#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/auth/public.h>

#include <yt/yt/library/tracing/jaeger/public.h>

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/rpc/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

//! Represents required environment interface to set up RPC proxy API service.
struct IBootstrap
{
    virtual ~IBootstrap() = default;

    virtual const IInvokerPtr& GetWorkerInvoker() const = 0;
    virtual const NRpc::IAuthenticatorPtr& GetRpcAuthenticator() const = 0;
    virtual TApiServiceConfigPtr GetConfigApiService() const = 0;
    virtual NAuth::TAuthenticationManagerConfigPtr GetConfigAuthenticationManager() const = 0;
    virtual TApiServiceDynamicConfigPtr GetDynamicConfigApiService() const = 0;
    virtual const NTracing::TSamplerPtr& GetTraceSampler() const = 0;
    virtual const IProxyCoordinatorPtr& GetProxyCoordinator() const = 0;
    virtual const IAccessCheckerPtr& GetAccessChecker() const = 0;
    virtual const NApi::NNative::IConnectionPtr& GetNativeConnection() const = 0;
    virtual const NApi::NNative::IClientPtr& GetNativeClient() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
