#include "hive_profiling_manager.h"

#include "bootstrap.h"
#include "config.h"
#include "config_manager.h"
#include "hydra_facade.h"
#include "multicell_manager.h"

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NCellMaster {

using namespace NConcurrency;

class THiveProfilingManager
    : public IHiveProfilingManager
{
public:
    explicit THiveProfilingManager(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    void Initialize() override
    {
        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
            BIND(&THiveProfilingManager::OnProfiling, MakeWeak(this)),
            TDynamicCellMasterConfig::DefaultHiveProfilingPeriod);
        ProfilingExecutor_->Start();

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(
            BIND(&THiveProfilingManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

private:
    TBootstrap* const Bootstrap_;
    TPeriodicExecutorPtr ProfilingExecutor_;

    void OnProfiling()
    {
        const auto& masterCellTags = Bootstrap_->GetMulticellManager()->GetRegisteredMasterCellTags();
        auto cellIdFilter = std::function(
            [&] (NElection::TCellId cellId) {
                auto cellTag = NObjectClient::CellTagFromId(cellId);
                return std::ranges::find(masterCellTags, cellTag) != masterCellTags.end();
            });

        Bootstrap_->GetHiveManager()->OnProfiling(cellIdFilter);
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        auto hiveProfilingPeriod = Bootstrap_->GetConfigManager()->GetConfig()->CellMaster->HiveProfilingPeriod;
        ProfilingExecutor_->SetPeriod(hiveProfilingPeriod);
    }
};

IHiveProfilingManagerPtr CreateHiveProfilingManager(TBootstrap* bootstrap)
{
    return New<THiveProfilingManager>(bootstrap);
}

} // namespace NYT::NCellMaster
