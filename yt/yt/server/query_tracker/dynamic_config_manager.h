#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

//! Manages dynamic configuration of the query tracker components by pulling it periodically from masters.
/*!
 *  \note
 *  Thread affinity: any
 */
class TDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TQueryTrackerServerDynamicConfig>
{
public:
    TDynamicConfigManager(
        const TQueryTrackerServerConfigPtr& queryTrackerConfig,
        NApi::IClientPtr client,
        IInvokerPtr invoker);
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
