#include "automaton.h"
#include "bootstrap.h"
#include "config_manager.h"
#include "hydra_facade.h"
#include "multicell_manager.h"
#include "config.h"

#include <yt/server/master/tablet_server/config.h>

#include <yt/core/misc/serialize.h>

#include <yt/core/ytree/ypath_proxy.h>

namespace NYT::NCellMaster {

using namespace NHydra;
using namespace NObjectClient;
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
        }
    }

    const TDynamicClusterConfigPtr& GetConfig() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return Config_;
    }

    void SetConfig(TDynamicClusterConfigPtr config)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        Config_ = std::move(config);
        ReplicateConfigToSecondaryMasters();

        NTracing::TNullTraceContextGuard nullTraceContext;
        ConfigChanged_.Fire();
    }

    DEFINE_SIGNAL(void(), ConfigChanged);

private:
    TDynamicClusterConfigPtr Config_ = New<TDynamicClusterConfig>();

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void Save(NCellMaster::TSaveContext& context) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Save;
        Save(context, *Config_);
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;
        Load(context, *Config_);
    }

    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto req = TYPathProxy::Set("//sys/@config");
        req->set_value(ConvertToYsonString(GetConfig()).GetData());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMaster(req, cellTag);
    }

    void ReplicateConfigToSecondaryMasters()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            auto req = TYPathProxy::Set("//sys/@config");
            req->set_value(ConvertToYsonString(GetConfig()).GetData());
            multicellManager->PostToSecondaryMasters(req);
        }
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

void TConfigManager::SetConfig(TDynamicClusterConfigPtr config)
{
    Impl_->SetConfig(std::move(config));
}

DELEGATE_SIGNAL(TConfigManager, void(), ConfigChanged, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
