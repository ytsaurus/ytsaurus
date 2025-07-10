#pragma once

#include "private.h"

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NOffshoreNodeProxy {

////////////////////////////////////////////////////////////////////////////////

//! Manages dynamic configuration of the queue agent components by pulling it periodically from masters.
/*!
 *  \note
 *  Thread affinity: any
 */
class TDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TOffshoreNodeProxyDynamicConfig>
{
public:
    TDynamicConfigManager(
        const TOffshoreNodeProxyBootstrapConfigPtr& OffshoreNodeProxyConfig,
        NApi::IClientPtr client,
        IInvokerPtr invoker);
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
