#pragma once

#include "public.h"

#include "config.h"

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

//! Manages dynamic configuration of RPC proxy
//! by pulling it periodically from masters.
/*!
 *  \note
 *  Thread affinity: any
 */
struct IDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TProxyDynamicConfig>
{
    using TDynamicConfigManagerBase<TProxyDynamicConfig>::TDynamicConfigManagerBase;

    virtual void Initialize() = 0;
};

DEFINE_REFCOUNTED_TYPE(IDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

IDynamicConfigManagerPtr CreateDynamicConfigManager(
    TProxyConfigPtr config,
    IProxyCoordinatorPtr proxyCoordinator,
    NApi::NNative::IConnectionPtr connection,
    IInvokerPtr controlInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
