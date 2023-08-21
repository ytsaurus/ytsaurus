#include "dynamic_config_manager.h"

#include "bootstrap.h"
#include "config.h"
#include "master_connector.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NClusterNode {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

TClusterNodeDynamicConfigManager::TClusterNodeDynamicConfigManager(IBootstrap* bootstrap)
    : TDynamicConfigManagerBase(
        TDynamicConfigManagerOptions{
            .ConfigPath = "//sys/cluster_nodes/@config",
            .Name = "ClusterNode",
            .ConfigIsTagged = true
        },
        bootstrap->GetConfig()->DynamicConfigManager,
        bootstrap->GetClient(),
        bootstrap->GetControlInvoker())
    , Bootstrap_(bootstrap)
{ }

TClusterNodeDynamicConfigManager::TClusterNodeDynamicConfigManager(TClusterNodeDynamicConfigPtr staticConfig)
    : TDynamicConfigManagerBase(staticConfig)
    , Bootstrap_(nullptr)
{ }

void TClusterNodeDynamicConfigManager::Start()
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

std::vector<TString> TClusterNodeDynamicConfigManager::GetInstanceTags() const
{
    return Bootstrap_->GetLocalDescriptor().GetTags();
}

DEFINE_REFCOUNTED_TYPE(TClusterNodeDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
