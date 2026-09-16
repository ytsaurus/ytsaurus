#include "table_config_manager.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

namespace NYT::NTabletNode {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

TTableDynamicConfigManager::TTableDynamicConfigManager(
    IBootstrap* bootstrap)
    : TDynamicConfigManagerBase(
        TDynamicConfigManagerOptions{
            .ConfigPath = "//sys/@config/tablet_manager",
            .Name = "MountConfig",
        },
        bootstrap->GetTabletNodeConfig()->TableConfigManager,
        bootstrap->GetClient(),
        bootstrap->GetControlInvoker())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
