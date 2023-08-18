#include "table_config_manager.h"
#include "bootstrap.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/tablet_node/config.h>

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
        bootstrap->GetConfig()->TabletNode->TableConfigManager,
        bootstrap->GetClient(),
        bootstrap->GetControlInvoker())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
