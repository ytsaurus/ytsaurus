#pragma once

#include "public.h"

#include "config.h"

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

//! Manages dynamic configuration of HTTP proxy
//! by pulling it periodically from masters.
/*!
 *  \note
 *  Thread affinity: any
 */
struct IDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TProxyDynamicConfig>
{
    using TDynamicConfigManagerBase<TProxyDynamicConfig>::TDynamicConfigManagerBase;
};

DEFINE_REFCOUNTED_TYPE(IDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

IDynamicConfigManagerPtr CreateDynamicConfigManager(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
