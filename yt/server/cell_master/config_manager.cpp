#include "automaton.h"
#include "bootstrap.h"
#include "config_manager.h"
#include "multicell_manager.h"
#include "config.h"

#include <yt/server/tablet_server/config.h>

#include <yt/core/misc/serialize.h>

#include <yt/core/ytree/ypath_proxy.h>

namespace NYT {
namespace NCellMaster {

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
        if (Bootstrap_->IsPrimaryMaster()) {
            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->SubscribeReplicateValuesToSecondaryMaster(
                BIND(&TImpl::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
        }
    }

    const TDynamicClusterConfigPtr& GetConfig() const
    {
        return Config_;
    }

    void SetConfig(TDynamicClusterConfigPtr config)
    {
        Config_ = std::move(config);
        ReplicateConfigToSecondaryMasters();
    }

private:
    TDynamicClusterConfigPtr Config_ = New<TDynamicClusterConfig>();

    void Save(NCellMaster::TSaveContext& context) const
    {
        using NYT::Save;
        Save(context, *Config_);
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        using NYT::Load;
        // COMPAT(savrus)
        if (context.GetVersion() >= 620) {
            Load(context, *Config_);
        }
    }

    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
    {
        auto req = TYPathProxy::Set("//sys/@config");
        req->set_value(ConvertToYsonString(GetConfig()).GetData());

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        multicellManager->PostToMaster(req, cellTag);
    }

    void ReplicateConfigToSecondaryMasters()
    {
        if (Bootstrap_->IsPrimaryMaster()) {
            auto req = TYPathProxy::Set("//sys/@config");
            req->set_value(ConvertToYsonString(GetConfig()).GetData());

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
