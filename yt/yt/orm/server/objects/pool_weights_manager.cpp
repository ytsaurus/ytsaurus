#include "pool_weights_manager.h"

#include "config.h"
#include "private.h"

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/config.h>
#include <yt/yt/orm/server/master/helpers.h>

#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NServer::NMaster;

////////////////////////////////////////////////////////////////////////////////

class TPoolWeightManager
    : public IPoolWeightManager
{
public:
    TPoolWeightManager(
        IBootstrap* bootstrap,
        TPoolWeightManagerConfigPtr initialConfig)
        : Bootstrap_(bootstrap)
    {
        SetConfig(initialConfig);
        Bootstrap_->SubscribeConfigUpdate(BIND(&TPoolWeightManager::OnConfigUpdate, MakeWeak(this)));
    }

    double GetWeight(const TString& poolName) override
    {
        return GetOrDefault(
            GetConfig()->FairSharePoolWeights,
            poolName,
            /*defaultWeight*/ 1.0);
    }

    bool IsFairShareEnabled() const override
    {
        return GetConfig()->EnableFairShareWorkerPool;
    }

    DEFINE_SIGNAL_OVERRIDE(void(const TPoolWeightManagerConfigPtr&), ConfigUpdate);

private:
    IBootstrap* const Bootstrap_;

    TAtomicIntrusivePtr<TPoolWeightManagerConfig> Config_;

    void SetConfig(TPoolWeightManagerConfigPtr config)
    {
        Config_.Store(std::move(config));
    }

    TPoolWeightManagerConfigPtr GetConfig() const
    {
        return Config_.Acquire();
    }

    void OnConfigUpdate(const TMasterDynamicConfigPtr& ormMasterConfig)
    {
        auto config = ormMasterConfig->PoolWeightManager;
        if (NOrm::NServer::NMaster::AreConfigsEqual(GetConfig(), config)) {
            return;
        }

        YT_LOG_INFO("Updating pool weight provider configuration");
        SetConfig(std::move(config));
        ConfigUpdate_.Fire(GetConfig());
    }
};

////////////////////////////////////////////////////////////////////////////////

IPoolWeightManagerPtr CreatePoolWeightManager(IBootstrap* bootstrap, TPoolWeightManagerConfigPtr config)
{
    return New<TPoolWeightManager>(bootstrap, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
