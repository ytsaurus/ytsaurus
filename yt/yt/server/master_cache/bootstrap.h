#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/library/containers/public.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
{
    virtual ~IBootstrap() = default;

    virtual void Initialize() = 0;
    virtual void Run() = 0;

    virtual const TMasterCacheConfigPtr& GetConfig() const = 0;
    virtual const NApi::NNative::IConnectionPtr& GetConnection() const = 0;
    virtual const NApi::IClientPtr& GetRootClient() const = 0;
    virtual const NYTree::IMapNodePtr& GetOrchidRoot() const = 0;
    virtual const NRpc::IServerPtr& GetRpcServer() const = 0;
    virtual const IInvokerPtr& GetControlInvoker() const = 0;
    virtual const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const = 0;
    virtual const TDynamicConfigManagerPtr& GetDynamicConfigManger() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TMasterCacheConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

class TBootstrapBase
    : public IBootstrap
{
public:
    explicit TBootstrapBase(IBootstrap* bootstrap);

    const TMasterCacheConfigPtr& GetConfig() const override;
    const NApi::NNative::IConnectionPtr& GetConnection() const override;
    const NApi::IClientPtr& GetRootClient() const override;
    const NYTree::IMapNodePtr& GetOrchidRoot() const override;
    const NRpc::IServerPtr& GetRpcServer() const override;
    const IInvokerPtr& GetControlInvoker() const override;
    const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const override;
    const TDynamicConfigManagerPtr& GetDynamicConfigManger() const override;

private:
    IBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
