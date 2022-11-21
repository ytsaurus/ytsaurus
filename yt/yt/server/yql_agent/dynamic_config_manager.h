#pragma once

#include "private.h"
#include "config.h"

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

//! Manages dynamic configuration of the yql agent components by pulling it periodically from masters.
/*!
 *  \note
 *  Thread affinity: any
 */
class TDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TYqlAgentServerDynamicConfig>
{
public:
    TDynamicConfigManager(
        const TYqlAgentServerConfigPtr& yqlAgentConfig,
        NApi::IClientPtr client,
        IInvokerPtr invoker);
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
