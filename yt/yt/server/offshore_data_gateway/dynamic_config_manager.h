#pragma once

#include "private.h"

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

//! Manages dynamic configuration of the offshore data gateway components by pulling it periodically from masters.
/*!
 *  \note
 *  Thread affinity: any
 */
class TDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TOffshoreDataGatewayDynamicConfig>
{
public:
    TDynamicConfigManager(
        const TOffshoreDataGatewayBootstrapConfigPtr& offshoreDataGatewayConfig,
        NApi::IClientPtr client,
        IInvokerPtr invoker);
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
