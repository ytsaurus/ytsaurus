#include "config_manager.h"

#include "alert_manager.h"
#include "automaton.h"
#include "bootstrap.h"
#include "hydra_facade.h"
#include "multicell_manager.h"
#include "serialize.h"
#include "config.h"

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ytree/ypath_proxy.h>

namespace NYT::NCellMaster {

using namespace NHydra;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TConfigManager
    : public IConfigManager
    , public TMasterAutomatonPart
{
public:
    explicit TConfigManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::ConfigManager)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default), AutomatonThread);

        RegisterLoader(
            "ConfigManager",
            BIND(&TConfigManager::Load, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "ConfigManager",
            BIND(&TConfigManager::Save, Unretained(this)));
    }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->SubscribeReplicateKeysToSecondaryMaster(
                BIND(&TConfigManager::OnReplicateKeysToSecondaryMaster, MakeWeak(this)));

            Bootstrap_->GetAlertManager()->RegisterAlertSource(
                BIND(&TConfigManager::GetAlerts, MakeStrong(this)));
        }

        // NB: Config Manager initialization is performed after all automaton parts registration in Hydra,
        // so config changed signal will be fired after other {LeaderRecoveryComplete,FollowerRecoveryComplete,LeaderActive}
        // subscribers. This property is crucial for many automaton parts.
        HydraManager_->SubscribeAutomatonLeaderRecoveryComplete(BIND(&TConfigManager::FireConfigChanged, MakeWeak(this)));
        HydraManager_->SubscribeAutomatonFollowerRecoveryComplete(BIND(&TConfigManager::FireConfigChanged, MakeWeak(this)));
        HydraManager_->SubscribeLeaderActive(BIND(&TConfigManager::FireConfigChanged, MakeWeak(this)));
    }

    const TDynamicClusterConfigPtr& GetConfig() const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        return Config_;
    }

    void SetConfig(INodePtr configNode) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto newConfig = New<TDynamicClusterConfig>();
        newConfig->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
        newConfig->Load(configNode);

        auto oldConfig = std::move(Config_);

        DoSetConfig(std::move(newConfig));

        ReplicateConfigToSecondaryMasters();

        HydraManager_->Reconfigure(Config_->HydraManager);

        NTracing::TNullTraceContextGuard nullTraceContext;
        ConfigChanged_.Fire(oldConfig);
    }

    DEFINE_SIGNAL_OVERRIDE(void(TDynamicClusterConfigPtr), ConfigChanged);

private:
    TDynamicClusterConfigPtr Config_ = New<TDynamicClusterConfig>();

    TError UnrecognizedOptionsAlert_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void Clear() override
    {
        Config_->SetDefaults();
        UnrecognizedOptionsAlert_ = {};
    }

    void DoSetConfig(TDynamicClusterConfigPtr newConfig)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        Config_ = std::move(newConfig);

        auto unrecognizedOptions = Config_->GetRecursiveUnrecognized();
        if (unrecognizedOptions->GetChildCount() > 0) {
            UnrecognizedOptionsAlert_ = TError("Found unrecognized options in dynamic cluster config")
                << TErrorAttribute("unrecognized_options", unrecognizedOptions);
        } else {
            UnrecognizedOptionsAlert_ = {};
        }
    }

    void Save(NCellMaster::TSaveContext& context) const
    {
        // No affinity annotation here since this could have been called
        // from a forked process.

        using NYT::Save;
        Save(context, *Config_);
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;

        auto newConfig = New<TDynamicClusterConfig>();
        newConfig->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
        Load(context, *newConfig);
        DoSetConfig(std::move(newConfig));
    }

    void OnReplicateKeysToSecondaryMaster(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto req = TYPathProxy::Set("//sys/@config");
        req->set_value(ConvertToYsonString(GetConfig()).ToString());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMaster(req, cellTag);
    }

    void ReplicateConfigToSecondaryMasters()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            auto req = TYPathProxy::Set("//sys/@config");
            req->set_value(ConvertToYsonString(GetConfig()).ToString());
            multicellManager->PostToSecondaryMasters(req);
        }
    }

    std::vector<TError> GetAlerts() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        std::vector<TError> alerts;
        if (!UnrecognizedOptionsAlert_.IsOK()) {
            alerts.push_back(UnrecognizedOptionsAlert_);
        }

        return alerts;
    }

    void FireConfigChanged()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(!HasMutationContext());

        // Wait for config to be applied, so that it has all overrides after LeaderActive call.
        WaitFor(HydraManager_->Reconfigure(Config_->HydraManager))
            .ThrowOnError();

        ConfigChanged_.Fire(Config_);
    }
};

////////////////////////////////////////////////////////////////////////////////

IConfigManagerPtr CreateConfigManager(TBootstrap* bootstrap)
{
    return New<TConfigManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
