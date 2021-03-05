#include "config.h"
#include "public.h"
#include "cellar.h"

#include <yt/yt/core/actions/invoker_util.h>

namespace NYT::NCellarAgent {

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

    virtual void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (const auto& config : Config_->Cellars) {
            auto cellar = CreateCellar(config, Bootstrap_);
            cellar->Initialize();
            Cellars_.emplace(config->Type, std::move(cellar));
        }
    }

    virtual ICellarPtr GetCellar(ECellarType type) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto cellar = FindCellar(type);

        if (!cellar) {
            THROW_ERROR_EXCEPTION("Cellar %Qlv is unavailable",
                type);
        }

        return cellar;
    }

    virtual ICellarPtr FindCellar(ECellarType type) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (auto it = Cellars_.find(type)) {
            return it->second;
        }

        return nullptr;
    }

    virtual void Reconfigure(TDynamicCellarManagerConfigPtr config) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (const auto& cellarConfig : config->Cellars) {
            if (auto it = Cellars_.find(cellarConfig->Type)) {
                it->second->Reconfigure(cellarConfig);
            }
        }
    }

protected:
    const TCellarManagerConfigPtr Config_;
    const ICellarBootstrapProxyPtr Bootstrap_;

    THashMap<ECellarType, ICellarPtr> Cellars_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
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
