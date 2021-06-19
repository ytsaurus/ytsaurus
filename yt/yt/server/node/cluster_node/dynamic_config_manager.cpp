#include "dynamic_config_manager.h"

#include "bootstrap.h"
#include "config.h"
#include "master_connector.h"
#include "private.h"

namespace NYT::NClusterNode {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

TClusterNodeDynamicConfigManager::TClusterNodeDynamicConfigManager(TBootstrap* bootstrap)
    : TDynamicConfigManagerBase(
        TDynamicConfigManagerOptions{
            .ConfigPath = "//sys/cluster_nodes/@config",
            .Name = "ClusterNode",
            .ConfigIsTagged = true
        },
        bootstrap->GetConfig()->DynamicConfigManager,
        bootstrap->GetMasterClient(),
        bootstrap->GetControlInvoker())
    , Bootstrap_(bootstrap)
{ }

void TClusterNodeDynamicConfigManager::Start()
{
    TDynamicConfigManagerBase::Start();

    Bootstrap_->GetClusterNodeMasterConnector()->SubscribePopulateAlerts(
        BIND([this, this_ = MakeStrong(this)] (std::vector<TError>* alerts) {
            auto errors = GetErrors();
            for (auto error : errors) {
                alerts->push_back(std::move(error));
            }
        }));
}

std::vector<TString> TClusterNodeDynamicConfigManager::GetInstanceTags() const
{
    return Bootstrap_->GetClusterNodeMasterConnector()->GetLocalDescriptor().GetTags();
}

DEFINE_REFCOUNTED_TYPE(TClusterNodeDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
