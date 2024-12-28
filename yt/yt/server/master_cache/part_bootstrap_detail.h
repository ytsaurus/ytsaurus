#pragma once

#include "part_bootstrap.h"

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

class TPartBootstrapBase
    : public IPartBootstrap
{
public:
    explicit TPartBootstrapBase(IBootstrap* bootstrap);

    const TMasterCacheBootstrapConfigPtr& GetConfig() const override;
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
