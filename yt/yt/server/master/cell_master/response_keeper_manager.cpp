#include "response_keeper_manager.h"

#include "automaton.h"
#include "config.h"
#include "config_manager.h"
#include "hydra_facade.h"
#include "private.h"
#include "serialize.h"

#include <yt/yt/server/master/cell_master/proto/response_keeper_manager.pb.h>

#include <yt/yt/server/lib/hydra/persistent_response_keeper.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NCellMaster {

using namespace NConcurrency;
using namespace NHydra;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellMasterLogger;

////////////////////////////////////////////////////////////////////////////////

class TResponseKeeperManager
    : public IResponseKeeperManager
    , public TMasterAutomatonPart
{
public:
    TResponseKeeperManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::ResponseKeeper)
        , ResponseKeeperEvictionExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::ResponseKeeper),
            BIND(&TResponseKeeperManager::OnEvict, MakeWeak(this))))
        , ResponseKeeper_(Bootstrap_->GetHydraFacade()->GetResponseKeeper())
    {
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "ResponseKeeperManager",
            BIND(&TResponseKeeperManager::Save, Unretained(this)));
        RegisterLoader(
            "ResponseKeeperManager",
            BIND(&TResponseKeeperManager::Load, Unretained(this)));
        // COMPAT(babenko)
        RegisterLoader(
            "TResponseKeeperManager",
            BIND(&TResponseKeeperManager::Load, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TResponseKeeperManager::HydraEvictKeptResponses, Unretained(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TResponseKeeperManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

private:
    const TPeriodicExecutorPtr ResponseKeeperEvictionExecutor_;

    const IPersistentResponseKeeperPtr ResponseKeeper_;

    void OnEvict()
    {
        TReqEvictKeptResponses request;
        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger));
    }

    void HydraEvictKeptResponses(TReqEvictKeptResponses* /*request*/)
    {
        const auto& config = Bootstrap_->GetConfigManager()->GetConfig()->CellMaster->ResponseKeeper;
        ResponseKeeper_->Evict(config->ExpirationTimeout, config->MaxResponseCountPerEvictionPass);
    }

    void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        ResponseKeeperEvictionExecutor_->Start();
    }

    void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        YT_UNUSED_FUTURE(ResponseKeeperEvictionExecutor_->Stop());
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();
        ResponseKeeperEvictionExecutor_->SetPeriod(
            config->CellMaster->ResponseKeeper->EvictionPeriod);
    }

    void Clear() override
    {
        TMasterAutomatonPart::Clear();

        ResponseKeeper_->Clear();
    }

    void SetZeroState() override
    {
        TMasterAutomatonPart::SetZeroState();

        ResponseKeeper_->Clear();
    }

    void Save(TSaveContext& context)
    {
        using NYT::Save;

        ResponseKeeper_->Save(context);
    }

    void Load(TLoadContext& context)
    {
        using NYT::Load;

        ResponseKeeper_->Load(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

IResponseKeeperManagerPtr CreateResponseKeeperManager(TBootstrap* bootstrap)
{
    return New<TResponseKeeperManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
