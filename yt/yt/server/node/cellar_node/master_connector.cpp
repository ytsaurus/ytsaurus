#include "master_connector.h"

#include "bootstrap.h"
#include "private.h"
#include "config.h"

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>
#include <yt/yt/server/node/cluster_node/master_heartbeat_reporter_base.h>

#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>
#include <yt/yt/server/lib/cellar_agent/master_connector_helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/cellar_node_tracker_client/cellar_node_tracker_service_proxy.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NCellarNode {

using namespace NCellMasterClient;
using namespace NApi::NNative;
using namespace NCellarAgent;
using namespace NCellMasterClient;
using namespace NCellarClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NHydra;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NCellarNodeTrackerClient;
using namespace NCellarNodeTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector
    : public IMasterConnector
    , public TMasterHeartbeatReporterBase
{
    DEFINE_SIGNAL_OVERRIDE(OnHeartbeatRequestedSignature, HeartbeatRequested);

public:
    explicit TMasterConnector(IBootstrap* bootstrap)
        : TMasterHeartbeatReporterBase(
            bootstrap,
            /*reportHeartbeatsToAllSecondaryMasters*/ true,
            CellarNodeLogger().WithTag("HeartbeatType: %v", ENodeHeartbeatType::Cellar))
        , Bootstrap_(bootstrap)
        , Config_(bootstrap->GetConfig()->CellarNode->MasterConnector)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);
    }

    void Initialize() override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        TMasterHeartbeatReporterBase::Initialize();

        Bootstrap_->SubscribeMasterConnected(BIND_NO_PROPAGATE(&TMasterConnector::OnMasterConnected, MakeWeak(this)));
        Bootstrap_->SubscribePopulateAlerts(BIND_NO_PROPAGATE(&TMasterConnector::PopulateAlerts, MakeWeak(this)));

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TMasterConnector::OnDynamicConfigChanged, MakeWeak(this)));

        Reconfigure(GetDynamicConfig()->HeartbeatExecutor.value_or(Config_->HeartbeatExecutor));
    }

    void ScheduleHeartbeat(TCellTag cellTag) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        Bootstrap_->GetControlInvoker()->Invoke(
            BIND([this, this_ = MakeStrong(this), cellTag] {
                StartNodeHeartbeatsToCell(cellTag);
            }));
    }

    TCellarNodeTrackerServiceProxy::TReqHeartbeatPtr BuildHeartbeatRequest(TCellTag cellTag) const
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(Bootstrap_->IsConnected());

        auto masterChannel = Bootstrap_->GetMasterChannel(cellTag);
        TCellarNodeTrackerServiceProxy proxy(std::move(masterChannel));
        auto heartbeatRequest = proxy.Heartbeat();
        heartbeatRequest->SetTimeout(GetDynamicConfig()->HeartbeatTimeout);

        heartbeatRequest->set_node_id(ToProto(Bootstrap_->GetNodeId()));

        const auto& cellarManager = Bootstrap_->GetCellarManager();

        for (auto cellarType : TEnumTraits<ECellarType>::GetDomainValues()) {
            if (auto cellar = cellarManager->FindCellar(cellarType)) {
                auto* cellarInfo = heartbeatRequest->add_cellars();
                cellarInfo->set_type(ToProto(cellarType));

                AddCellarInfoToHeartbeatRequest(
                    cellarType,
                    cellar,
                    Bootstrap_->IsReadOnly(),
                    cellarInfo);

                // Populate slot info with tablet preload statistics.
                HeartbeatRequested_.Fire(cellarType, cellar, cellarInfo);
            }
        }

        return heartbeatRequest;
    }

    void OnHeartbeatSucceeded(const TCellarNodeTrackerServiceProxy::TRspHeartbeatPtr& response)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        const auto& cellarManager = Bootstrap_->GetCellarManager();
        std::optional<ECellarType> singleCellarType;

        for (const auto& cellarInfo : response->cellars()) {
            auto cellarType = FromProto<ECellarType>(cellarInfo.type());
            auto cellar = cellarManager->FindCellar(cellarType);
            UpdateCellarFromHeartbeatResponse(cellarType, cellar, cellarInfo);

            if (response->cellars().size() == 1) {
                singleCellarType = cellarType;
            }
        }

        if (singleCellarType) {
            static const TString tabletCellBundleTagName("tablet_cell_bundle");
            static const TString cellBundleTagName("cell_bundle");
            auto cellarType = *singleCellarType;

            SolomonTagAlert_ = UpdateSolomonTags(
                cellarManager,
                cellarType,
                cellarType == ECellarType::Tablet ? tabletCellBundleTagName : cellBundleTagName);
        }
    }

protected:
    TFuture<void> DoReportHeartbeat(TCellTag cellTag) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (!Bootstrap_->IsConnected()) {
            return MakeFuture(TError("Node disconnected"));
        }

        auto req = BuildHeartbeatRequest(cellTag);
        auto rspFuture = req->Invoke();
        EmplaceOrCrash(CellTagToHeartbeatRspFuture_, cellTag, rspFuture);
        return rspFuture.AsVoid();
    }

    void OnHeartbeatSucceeded(TCellTag cellTag) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto rspOrError = GetHeartbeatResponseOrError(cellTag);
        YT_VERIFY(rspOrError.IsOK());

        OnHeartbeatSucceeded(rspOrError.Value());
    }

    void OnHeartbeatFailed(TCellTag cellTag) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto rspOrError = GetHeartbeatResponseOrError(cellTag);
        YT_VERIFY(!rspOrError.IsOK());
    }

    void ResetState(TCellTag cellTag) override
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        CellTagToHeartbeatRspFuture_.erase(cellTag);
    }

private:
    IBootstrap* const Bootstrap_;
    const TMasterConnectorConfigPtr Config_;
    TError SolomonTagAlert_;

    THashMap<TCellTag, TFuture<TCellarNodeTrackerServiceProxy::TRspHeartbeatPtr>> CellTagToHeartbeatRspFuture_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void PopulateAlerts(std::vector<TError>* alerts) const
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        if (!SolomonTagAlert_.IsOK()) {
            alerts->push_back(SolomonTagAlert_);
        }
    }

    void OnMasterConnected(TNodeId /*nodeId*/)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        StartNodeHeartbeats();
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        Reconfigure(newNodeConfig->CellarNode->MasterConnector->HeartbeatExecutor.value_or(Config_->HeartbeatExecutor));

        YT_LOG_INFO("Dynamic config changed");
    }

    TMasterConnectorDynamicConfigPtr GetDynamicConfig() const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetDynamicConfigManager()->GetConfig()->CellarNode->MasterConnector;
    }

    TErrorOr<TCellarNodeTrackerServiceProxy::TRspHeartbeatPtr> GetHeartbeatResponseOrError(TCellTag cellTag)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        auto futureIt = GetIteratorOrCrash(CellTagToHeartbeatRspFuture_, cellTag);
        auto future = std::move(futureIt->second);
        CellTagToHeartbeatRspFuture_.erase(futureIt);
        YT_VERIFY(future.IsSet());

        return future.Get();
    }
};

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap)
{
    return New<TMasterConnector>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
