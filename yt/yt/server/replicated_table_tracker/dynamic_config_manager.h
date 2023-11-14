#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_server/public.h>

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NReplicatedTableTracker {

////////////////////////////////////////////////////////////////////////////////

class TDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<NTabletServer::TDynamicReplicatedTableTrackerConfig>
{
public:
    TDynamicConfigManager(
        NDynamicConfig::TDynamicConfigManagerConfigPtr config,
        NApi::NNative::IClientPtr client,
        IInvokerPtr invoker);
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
