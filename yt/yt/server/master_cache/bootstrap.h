#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/bootstrap.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/library/containers/public.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrapBase
    : public virtual TRefCounted
{
    virtual const TMasterCacheBootstrapConfigPtr& GetConfig() const = 0;
    virtual const NApi::NNative::IConnectionPtr& GetConnection() const = 0;
    virtual const NApi::IClientPtr& GetRootClient() const = 0;
    virtual const NYTree::IMapNodePtr& GetOrchidRoot() const = 0;
    virtual const NRpc::IServerPtr& GetRpcServer() const = 0;
    virtual const IInvokerPtr& GetControlInvoker() const = 0;
    virtual const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const = 0;
    virtual const TDynamicConfigManagerPtr& GetDynamicConfigManger() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
    : public IBootstrapBase
    , public NServer::IDaemonBootstrap
{
    virtual const TMasterCacheBootstrapConfigPtr& GetConfig() const = 0;
    virtual const NApi::NNative::IConnectionPtr& GetConnection() const = 0;
    virtual const NApi::IClientPtr& GetRootClient() const = 0;
    virtual const NYTree::IMapNodePtr& GetOrchidRoot() const = 0;
    virtual const NRpc::IServerPtr& GetRpcServer() const = 0;
    virtual const IInvokerPtr& GetControlInvoker() const = 0;
    virtual const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const = 0;
    virtual const TDynamicConfigManagerPtr& GetDynamicConfigManger() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateMasterCacheBootstrap(
    TMasterCacheBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
