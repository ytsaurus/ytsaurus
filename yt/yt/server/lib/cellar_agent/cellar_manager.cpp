#include "cellar_manager.h"

#include "bootstrap_proxy.h"
#include "cellar.h"
#include "config.h"
#include "private.h"
#include "public.h"

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NCellarAgent {

using namespace NCellarClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellarAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellarManager
    : public ICellarManager
{
public:
    TCellarManager(
        TCellarManagerConfigPtr config,
        ICellarBootstrapProxyPtr bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(std::move(bootstrap))
    { }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Bootstrap_->SubscribePopulateAlerts(BIND(&TCellarManager::PopulateAlerts, MakeWeak(this)));

        for (const auto& [type, config] : Config_->Cellars) {
            auto cellar = CreateCellar(type, config, Bootstrap_);
            cellar->Initialize();
            Cellars_.emplace(type, std::move(cellar));
        }
    }

    ICellarPtr GetCellar(ECellarType type) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto cellar = FindCellar(type);
        YT_VERIFY(cellar);
        return cellar;
    }

    ICellarPtr FindCellar(ECellarType type) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (auto it = Cellars_.find(type)) {
            return it->second;
        }

        return nullptr;
    }

    void Reconfigure(TCellarManagerDynamicConfigPtr config) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // TODO(savrus) Remove when reconfiguration is deployed and verified.
        YT_LOG_DEBUG("Reconfiguring cellar manager (NewConfig: %v)",
            ConvertToYsonString(config, EYsonFormat::Text).AsStringBuf());

        THashSet<ECellarType> updatedCellarTypes;
        for (const auto& [type, cellarConfig] : config->Cellars) {
            if (auto cellar = FindCellar(type)) {
                cellar->Reconfigure(cellarConfig);
                updatedCellarTypes.insert(type);
            }
        }

        for (const auto& [type, cellarConfig] : Config_->Cellars) {
            if (!updatedCellarTypes.contains(type)) {
                auto newConfig = New<TCellarDynamicConfig>();
                auto cellar = GetCellar(type);
                cellar->Reconfigure(newConfig);
            }
        }
    }

private:
    const TCellarManagerConfigPtr Config_;
    const ICellarBootstrapProxyPtr Bootstrap_;

    THashMap<ECellarType, ICellarPtr> Cellars_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    void PopulateAlerts(std::vector<TError>* alerts)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (const auto& [cellarType, cellar] : Cellars_) {
            cellar->PopulateAlerts(alerts);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ICellarManagerPtr CreateCellarManager(
    TCellarManagerConfigPtr config,
    ICellarBootstrapProxyPtr bootstrap)
{
    return New<TCellarManager>(
        std::move(config),
        std::move(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
