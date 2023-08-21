#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTcpProxy {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
{
    virtual ~IBootstrap() = default;

    virtual void Initialize() = 0;
    virtual void Run() = 0;

    virtual const TTcpProxyConfigPtr& GetConfig() const = 0;

    virtual const TDynamicConfigManagerPtr& GetDynamicConfigManager() const = 0;

    virtual const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const = 0;

    virtual const IInvokerPtr& GetControlInvoker() const = 0;

    virtual const NApi::NNative::IConnectionPtr& GetNativeConnection() const = 0;
    virtual const NApi::NNative::IClientPtr& GetNativeClient() const = 0;
    virtual NApi::IClientPtr GetRootClient() const = 0;

    virtual NConcurrency::IPollerPtr GetPoller() const = 0;
    virtual NConcurrency::IPollerPtr GetAcceptor() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TTcpProxyConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTcpProxy
