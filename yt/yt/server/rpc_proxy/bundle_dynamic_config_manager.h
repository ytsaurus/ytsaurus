#pragma once

#include "public.h"

#include "config.h"

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class IBundleDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TBundleProxyDynamicConfig>
{
public:
    using TDynamicConfigManagerBase<TBundleProxyDynamicConfig>::TDynamicConfigManagerBase;

    virtual void Initialize() = 0;
};

DEFINE_REFCOUNTED_TYPE(IBundleDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

IBundleDynamicConfigManagerPtr CreateBundleDynamicConfigManager(
    TProxyConfigPtr config,
    IProxyCoordinatorPtr proxyCoordinator,
    NApi::NNative::IConnectionPtr connection,
    IInvokerPtr controlInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
