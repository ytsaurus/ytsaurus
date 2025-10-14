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
using namespace NCypressClient;
using namespace NObjectClient;

class THiveProfilingManager
    : public IHiveProfilingManager
{
public:
    explicit THiveProfilingManager(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , ProfilingExecutor_(
            New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
                BIND(&THiveProfilingManager::OnProfiling, MakeWeak(this)),
                TDynamicCellMasterConfig::DefaultHiveProfilingPeriod))
    { }

    void Initialize() override
    {
        ProfilingExecutor_->Start();

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(
            BIND(&THiveProfilingManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

private:
    TBootstrap* const Bootstrap_;
    const TPeriodicExecutorPtr ProfilingExecutor_;

    void OnProfiling()
    {
        auto cellIdFilter = [] (NElection::TCellId cellId) {
            return TypeFromId(cellId) == EObjectType::MasterCell;
        };

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
