#include "master_connector.h"

#include "bootstrap.h"
#include "private.h"
#include "config.h"

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

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
using namespace NObjectClient;
using namespace NCellarNodeTrackerClient;
using namespace NCellarNodeTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = CellarNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector
    : public IMasterConnector
{
    DEFINE_SIGNAL_OVERRIDE(OnHeartbeatRequestedSignature, HeartbeatRequested);

public:
    explicit TMasterConnector(IBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Config_(bootstrap->GetConfig()->CellarNode->MasterConnector)
        , HeartbeatPeriod_(Config_->HeartbeatPeriod)
        , HeartbeatPeriodSplay_(Config_->HeartbeatPeriodSplay)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
    }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Bootstrap_->SubscribeMasterConnected(BIND_NO_PROPAGATE(&TMasterConnector::OnMasterConnected, MakeWeak(this)));
        Bootstrap_->SubscribeMasterDisconnected(BIND_NO_PROPAGATE(&TMasterConnector::OnMasterDisconnected, MakeWeak(this)));
        Bootstrap_->SubscribePopulateAlerts(BIND_NO_PROPAGATE(&TMasterConnector::PopulateAlerts, MakeWeak(this)));
        Bootstrap_->SubscribeReadyToReportHeartbeatsToNewMasters(
            BIND_NO_PROPAGATE(&TMasterConnector::OnReadyToReportHeartbeatsToNewMasters, MakeStrong(this))
                .Via(Bootstrap_->GetControlInvoker()));

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TMasterConnector::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void ScheduleHeartbeat(TCellTag cellTag, bool immediately) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Bootstrap_->GetControlInvoker()->Invoke(
            BIND([this, this_ = MakeStrong(this), cellTag, immediately] {
                YT_UNUSED_FUTURE(DoScheduleHeartbeat(cellTag, immediately));
            }));
    }

    TReqHeartbeat GetHeartbeatRequest(TCellTag /*cellTag*/) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(Bootstrap_->IsConnected());

        TReqHeartbeat heartbeatRequest;
        heartbeatRequest.set_node_id(ToProto<ui32>(Bootstrap_->GetNodeId()));

        const auto& cellarManager = Bootstrap_->GetCellarManager();

        for (auto cellarType : TEnumTraits<ECellarType>::GetDomainValues()) {
            if (auto cellar = cellarManager->FindCellar(cellarType)) {
                auto* cellarInfo = heartbeatRequest.add_cellars();
                cellarInfo->set_type(ToProto<int>(cellarType));

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

    void OnHeartbeatResponse(const TRspHeartbeat& response)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& cellarManager = Bootstrap_->GetCellarManager();
        std::optional<ECellarType> singleCellarType;

        for (const auto& cellarInfo : response.cellars()) {
            auto cellarType = FromProto<ECellarType>(cellarInfo.type());
            auto cellar = cellarManager->FindCellar(cellarType);
            UpdateCellarFromHeartbeatResponse(cellarType, cellar, cellarInfo);

            if (response.cellars().size() == 1) {
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

private:
    IBootstrap* const Bootstrap_;
    const TMasterConnectorConfigPtr Config_;

    IInvokerPtr HeartbeatInvoker_;
    TDuration HeartbeatPeriod_;
    TDuration HeartbeatPeriodSplay_;

    struct TPerCellTagData
    {
        TAsyncReaderWriterLock HeartbeatLock;
        int ScheduledHeartbeatCount = 0;
    };
    THashMap<TCellTag, TPerCellTagData> PerCellTagData_;

    TError SolomonTagAlert_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void PopulateAlerts(std::vector<TError>* alerts) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!SolomonTagAlert_.IsOK()) {
            alerts->push_back(SolomonTagAlert_);
        }
    }

    void OnMasterConnected(TNodeId /*nodeId*/)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        HeartbeatInvoker_ = Bootstrap_->GetMasterConnectionInvoker();

        StartHeartbeats();
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (auto& [_, data] : PerCellTagData_) {
            data.ScheduledHeartbeatCount = 0;
        }
    }

    void OnReadyToReportHeartbeatsToNewMasters(const TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector();
        std::vector<TFuture<bool>> futures;
        std::vector<TCellTag> newSecondaryMasterCellTags;
        futures.reserve(newSecondaryMasterConfigs.size());
        newSecondaryMasterCellTags.reserve(newSecondaryMasterConfigs.size());
        for (const auto& [cellTag, _] : newSecondaryMasterConfigs) {
            newSecondaryMasterCellTags.emplace_back(cellTag);
            if (clusterNodeMasterConnector->IsRegisteredAtPrimaryMaster()) {
                futures.push_back(BIND([this, weakThis = MakeWeak(this), cellTag = cellTag] {
                    VERIFY_THREAD_AFFINITY(ControlThread);

                    if (auto this_ = weakThis.Lock()) {
                        return DoScheduleHeartbeat(cellTag, /*immediately*/ false);
                    } else {
                        return MakeFuture(false);
                    }
                }).AsyncVia(HeartbeatInvoker_).Run());
            }
        }

        auto resultsOrError = WaitFor(AllSucceeded(std::move(futures)));
        YT_LOG_ALERT_UNLESS(
            resultsOrError.IsOK(),
            resultsOrError,
            "Failed to report cellar node heartbeat to new masters "
            "(NewCellTags: %v)",
            newSecondaryMasterCellTags);

        if (resultsOrError.IsOK()) {
            auto results = resultsOrError.Value();
            YT_LOG_WARNING_UNLESS(
                AllOf(results, [] (auto result) { return result; }),
                "Some of cellar heartbeats failed, node will re-register at primary master "
                "(NewCellTags: %v)",
                newSecondaryMasterCellTags);
        }

        YT_LOG_INFO(
            "Received master cell directory change, attempted to report heartbeats to new masters "
            "(NewCellTags: %v)",
            newSecondaryMasterCellTags);
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        HeartbeatPeriod_ = newNodeConfig->CellarNode->MasterConnector->HeartbeatPeriod.value_or(Config_->HeartbeatPeriod);
        HeartbeatPeriodSplay_ = newNodeConfig->CellarNode->MasterConnector->HeartbeatPeriodSplay.value_or(Config_->HeartbeatPeriodSplay);
    }

    void StartHeartbeats()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Starting cellar node heartbeats");

        const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector();
        for (auto cellTag : clusterNodeMasterConnector->GetMasterCellTags()) {
            YT_UNUSED_FUTURE(DoScheduleHeartbeat(cellTag, /*immediately*/ true));
        }
    }

    TFuture<bool> DoScheduleHeartbeat(TCellTag cellTag, bool immediately)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ++PerCellTagData_[cellTag].ScheduledHeartbeatCount;

        auto delay = immediately ? TDuration::Zero() : HeartbeatPeriod_ + RandomDuration(HeartbeatPeriodSplay_);
        return TDelayedExecutor::MakeDelayed(delay, HeartbeatInvoker_)
            .Apply(BIND([this, this_ = MakeStrong(this), cellTag] (const TErrorOr<void>& error) {
                if (error.IsOK()) {
                    return ReportHeartbeat(cellTag);
                }
                return MakeFuture<bool>(error);
            }).AsyncVia(HeartbeatInvoker_));
    }

    TFuture<bool> ReportHeartbeat(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&PerCellTagData_[cellTag].HeartbeatLock))
            .ValueOrThrow();

        --PerCellTagData_[cellTag].ScheduledHeartbeatCount;

        auto masterChannel = Bootstrap_->GetMasterChannel(cellTag);
        TCellarNodeTrackerServiceProxy proxy(masterChannel);

        auto req = proxy.Heartbeat();
        req->SetTimeout(GetDynamicConfig()->HeartbeatTimeout);

        static_cast<TReqHeartbeat&>(*req) = GetHeartbeatRequest(cellTag);

        YT_LOG_INFO("Sending cellar node heartbeat to master (CellTag: %v)",
            cellTag);

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.IsOK()) {
            OnHeartbeatResponse(*rspOrError.Value());

            YT_LOG_INFO("Successfully reported cellar node heartbeat to master (CellTag: %v)",
                cellTag);

            // Schedule next heartbeat if no more heartbeats are scheduled.
            if (PerCellTagData_[cellTag].ScheduledHeartbeatCount == 0) {
                YT_UNUSED_FUTURE(DoScheduleHeartbeat(cellTag, /*immediately*/ false));
            }
            return MakeFuture(true);
        } else {
            YT_LOG_WARNING(rspOrError, "Error reporting cellar node heartbeat to master (CellTag: %v)",
                cellTag);
            if (IsRetriableError(rspOrError) || rspOrError.FindMatching(HeartbeatRetriableErrors)) {
                return DoScheduleHeartbeat(cellTag, /*immediately*/ false);
            } else {
                YT_LOG_INFO(rspOrError, "Node will reset connection to masters, failed to report heartbeat to master cell (CellTag: %v)",
                    cellTag);
                Bootstrap_->ResetAndRegisterAtMaster();
            }
            return MakeFuture(false);
        }
    }

    TMasterConnectorDynamicConfigPtr GetDynamicConfig() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetDynamicConfigManager()->GetConfig()->CellarNode->MasterConnector;
    }
};

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap)
{
    return New<TMasterConnector>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode
