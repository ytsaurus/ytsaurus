#pragma once

#include "private.h"
#include "config.h"

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NChaosCache {

////////////////////////////////////////////////////////////////////////////////

//! Manages dynamic configuration of a chaos cache
//! by pulling it periodically from masters.
/*!
 *  \note
 *  Thread affinity: any
 */
class TDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TChaosCacheDynamicConfig>
{
public:
    explicit TDynamicConfigManager(IBootstrap* bootstrap);
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosCache
