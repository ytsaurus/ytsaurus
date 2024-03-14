#include "alert_manager.h"

#include "private.h"
#include "automaton.h"
#include "config.h"
#include "config_manager.h"
#include "hydra_facade.h"
#include "multicell_manager.h"
#include "serialize.h"

#include <yt/yt/server/master/cell_master/proto/alert_manager.pb.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NCellMaster {

using namespace NCellMaster::NProto;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NHydra;
using namespace NYTree;

using NObjectServer::TCellTag;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellMasterLogger;

////////////////////////////////////////////////////////////////////////////////

class TAlertManager
    : public IAlertManager
    , public TMasterAutomatonPart
{
public:
    explicit TAlertManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::Default)
        , UpdateAlertsExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
            BIND(&TAlertManager::UpdateAlerts, MakeWeak(this))))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default), AutomatonThread);

        RegisterLoader(
            "AlertManager",
            BIND(&TAlertManager::Load, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Values,
            "AlertManager",
            BIND(&TAlertManager::Save, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TAlertManager::HydraSetCellAlerts, Unretained(this)));
    }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Bootstrap_->GetConfigManager()->SubscribeConfigChanged(BIND(&TAlertManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void RegisterAlertSource(TAlertSource alertSource) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        AlertSources_.push_back(alertSource);
    }

    std::vector<TError> GetAlerts() const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        std::vector<TError> alerts;
        for (const auto& [cellTag, cellAlerts] : CellTagToAlerts_) {
            alerts.insert(alerts.end(), cellAlerts.begin(), cellAlerts.end());
        }

        return alerts;
    }

private:
    const TPeriodicExecutorPtr UpdateAlertsExecutor_;

    THashMap<TCellTag, std::vector<TError>> CellTagToAlerts_;

    std::vector<TAlertSource> AlertSources_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void HydraSetCellAlerts(TReqSetCellAlerts* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        YT_VERIFY(multicellManager->IsPrimaryMaster());

        auto cellTag = FromProto<TCellTag>(request->cell_tag());
        auto alerts = FromProto<std::vector<TError>>(request->alerts());

        if (cellTag == multicellManager->GetCellTag()) {
            YT_LOG_DEBUG(
                "Updating primary master alerts (CellTag: %v, AlertCount: %v)",
                cellTag,
                request->alerts_size());
        } else {
            YT_LOG_DEBUG(
                "Received alerts from secondary master (CellTag: %v, AlertCount: %v)",
                cellTag,
                request->alerts_size());
        }

        CellTagToAlerts_[cellTag] = std::move(alerts);
    }

    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        UpdateAlertsExecutor_->Start();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        YT_UNUSED_FUTURE(UpdateAlertsExecutor_->Stop());
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        UpdateAlertsExecutor_->SetPeriod(
            Bootstrap_->GetConfigManager()->GetConfig()->CellMaster->AlertUpdatePeriod);
    }

    void Load(TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;

        Load(context, CellTagToAlerts_);
    }

    void Save(TSaveContext& context)
    {
        // No affinity annotation here since this could have been called
        // from a forked process.

        using NYT::Save;

        Save(context, CellTagToAlerts_);
    }

    void UpdateAlerts()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        YT_LOG_DEBUG("Updating master alerts");

        std::vector<TError> localAlerts;
        for (const auto& alertSource : AlertSources_) {
            auto alerts = alertSource();
            localAlerts.insert(localAlerts.end(), alerts.begin(), alerts.end());
        }

        for (const auto& alert : localAlerts) {
            YT_VERIFY(!alert.IsOK());
            YT_LOG_WARNING(alert, "Registered master alert");
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        TReqSetCellAlerts request;
        request.set_cell_tag(ToProto<int>(multicellManager->GetCellTag()));
        ToProto(request.mutable_alerts(), localAlerts);

        if (multicellManager->IsPrimaryMaster()) {
            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger));
        } else {
            multicellManager->PostToPrimaryMaster(request, /*reliable*/ false);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IAlertManagerPtr CreateAlertManager(TBootstrap* bootstrap)
{
    return New<TAlertManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
