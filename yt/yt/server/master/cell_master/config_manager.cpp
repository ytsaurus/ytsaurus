#include "alert_manager.h"
#include "automaton.h"
#include "bootstrap.h"
#include "config_manager.h"
#include "hydra_facade.h"
#include "multicell_manager.h"
#include "serialize.h"
#include "config.h"

// COMPAT(gritukan)
#include "serialize.h"

#include <yt/server/master/tablet_server/config.h>

#include <yt/core/misc/serialize.h>

#include <yt/core/ytree/ypath_proxy.h>

namespace NYT::NCellMaster {

using namespace NHydra;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TConfigManager::TImpl
    : public TMasterAutomatonPart
{
public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::ConfigManager)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default), AutomatonThread);

        RegisterLoader(
            "ConfigManager",
            BIND(&TImpl::Load, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "ConfigManager",
            BIND(&TImpl::Save, Unretained(this)));
    }

    void Initialize()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->SubscribeReplicateKeysToSecondaryMaster(
                BIND(&TImpl::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));

            Bootstrap_->GetAlertManager()->RegisterAlertSource(
                BIND(&TImpl::GetAlerts, MakeStrong(this)));
        }
    }

    const TDynamicClusterConfigPtr& GetConfig() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return Config_;
    }

    void SetConfig(INodePtr configNode)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto newConfig = New<TDynamicClusterConfig>();
        newConfig->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
        newConfig->Load(configNode);

        auto oldConfig = std::move(Config_);
        Config_ = std::move(newConfig);

        auto unrecognizedOptions = Config_->GetUnrecognizedRecursively();
        if (unrecognizedOptions->GetChildCount() > 0) {
            UnrecognizedOptionsAlert_ = TError("Found unrecognized options in dynamic cluster config")
                << TErrorAttribute("unrecognized_options", ConvertToYsonString(unrecognizedOptions, EYsonFormat::Text));
        } else {
            UnrecognizedOptionsAlert_ = TError();
        }

        ReplicateConfigToSecondaryMasters();

        NTracing::TNullTraceContextGuard nullTraceContext;
        ConfigChanged_.Fire(oldConfig);
    }

    DEFINE_SIGNAL(void(TDynamicClusterConfigPtr), ConfigChanged);

private:
    TDynamicClusterConfigPtr Config_ = New<TDynamicClusterConfig>();

    TError UnrecognizedOptionsAlert_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void Save(NCellMaster::TSaveContext& context) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Save;
        Save(context, *Config_);
        Save(context, UnrecognizedOptionsAlert_);
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;
        Load(context, *Config_);

        // COMPAT(gritukan)
        if (context.GetVersion() >= EMasterReign::MasterAlerts) {
            Load(context, UnrecognizedOptionsAlert_);
        }
    }

    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
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
};

////////////////////////////////////////////////////////////////////////////////

TConfigManager::TConfigManager(
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TConfigManager::~TConfigManager() = default;

void TConfigManager::Initialize()
{
    Impl_->Initialize();
}

const TDynamicClusterConfigPtr& TConfigManager::GetConfig() const
{
    return Impl_->GetConfig();
}

void TConfigManager::SetConfig(INodePtr configNode)
{
    Impl_->SetConfig(std::move(configNode));
}

DELEGATE_SIGNAL(TConfigManager, void(TDynamicClusterConfigPtr), ConfigChanged, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
