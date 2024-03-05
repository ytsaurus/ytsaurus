#include "bundle_dynamic_config_manager.h"

#include "bootstrap.h"
#include "config.h"
#include "master_connector.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NCellarNode {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

static TDynamicConfigManagerConfigPtr MakeManagerConfig(NClusterNode::IBootstrap* bootstrap)
{
    auto config = CloneYsonStruct(bootstrap->GetConfig()->DynamicConfigManager);
    config->IgnoreConfigAbsence = true;
    return config;
}

////////////////////////////////////////////////////////////////////////////////

TBundleDynamicConfigManager::TBundleDynamicConfigManager(NClusterNode::IBootstrap* bootstrap)
    : TDynamicConfigManagerBase(
        TDynamicConfigManagerOptions{
            .ConfigPath = "//sys/tablet_cell_bundles/@config",
            .Name = "TabletCellBundle",
            .ConfigIsTagged = true
        },
        MakeManagerConfig(bootstrap),
        bootstrap->GetClient(),
        bootstrap->GetControlInvoker())
    , Bootstrap_(bootstrap)
{ }

TBundleDynamicConfigManager::TBundleDynamicConfigManager(TBundleDynamicConfigPtr staticConfig)
    : TDynamicConfigManagerBase(std::move(staticConfig))
    , Bootstrap_(nullptr)
{ }

void TBundleDynamicConfigManager::Start()
{
    TDynamicConfigManagerBase::Start();

    Bootstrap_->SubscribePopulateAlerts(
        BIND([this, this_ = MakeStrong(this)] (std::vector<TError>* alerts) {
            auto errors = GetErrors();
            for (auto error : errors) {
                alerts->push_back(std::move(error));
            }
        }));
}

std::vector<TString> TBundleDynamicConfigManager::GetInstanceTags() const
{
    return Bootstrap_->GetLocalDescriptor().GetTags();
}

DEFINE_REFCOUNTED_TYPE(TBundleDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
