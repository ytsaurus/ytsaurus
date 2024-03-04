#include "master_connector.h"

#include "bootstrap.h"
#include "private.h"
#include "config.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>
#include <yt/yt/server/lib/cellar_agent/master_connector_helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/cellar_node_tracker_client/cellar_node_tracker_service_proxy.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/rpc/response_keeper.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NCellarNode {

using namespace NCellMasterClient;
using namespace NCellarAgent;
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

static const auto& Logger = CellarNodeLogger;

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

        Bootstrap_->SubscribeMasterConnected(BIND(&TMasterConnector::OnMasterConnected, MakeWeak(this)));
        Bootstrap_->SubscribeMasterDisconnected(BIND(&TMasterConnector::OnMasterDisconnected, MakeWeak(this)));
        Bootstrap_->SubscribePopulateAlerts(BIND(&TMasterConnector::PopulateAlerts, MakeWeak(this)));

        const auto& connection = Bootstrap_->GetClient()->GetNativeConnection();
        connection->GetMasterCellDirectory()->SubscribeCellDirectoryChanged(
            BIND(&TMasterConnector::OnMasterCellDirectoryChanged, MakeStrong(this))
                .Via(Bootstrap_->GetControlInvoker()));

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TMasterConnector::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void ScheduleHeartbeat(TCellTag cellTag, bool immediately) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Bootstrap_->GetControlInvoker()->Invoke(
            BIND(&TMasterConnector::DoScheduleHeartbeat, MakeWeak(this), cellTag, immediately));
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

    void OnMasterCellDirectoryChanged(
        const THashSet<TCellTag>& addedSecondaryCellTags,
        const TSecondaryMasterConnectionConfigs& /*reconfiguredSecondaryMasterConfigs*/,
        const THashSet<TCellTag>& removedSecondaryTags)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG_UNLESS(
            addedSecondaryCellTags.empty() && removedSecondaryTags.empty(),
            "Unexpected master cell configuration detected "
            "(AddedCellTags: %v, RemovedCellTags: %v)",
            addedSecondaryCellTags,
            removedSecondaryTags);
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

        for (auto cellTag : Bootstrap_->GetMasterCellTags()) {
            DoScheduleHeartbeat(cellTag, /*immediately*/ true);
        }
    }

    void DoScheduleHeartbeat(TCellTag cellTag, bool immediately)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ++PerCellTagData_[cellTag].ScheduledHeartbeatCount;

        auto delay = immediately ? TDuration::Zero() : HeartbeatPeriod_ + RandomDuration(HeartbeatPeriodSplay_);
        TDelayedExecutor::Submit(
            BIND(&TMasterConnector::ReportHeartbeat, MakeWeak(this), cellTag),
            delay,
            HeartbeatInvoker_);
    }

    void ReportHeartbeat(TCellTag cellTag)
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
                DoScheduleHeartbeat(cellTag, /*immediately*/ false);
            }
        } else {
            YT_LOG_WARNING(rspOrError, "Error reporting cellar node heartbeat to master (CellTag: %v)",
                cellTag);
            if (IsRetriableError(rspOrError)) {
                DoScheduleHeartbeat(cellTag, /*immediately*/ false);
            } else {
                Bootstrap_->ResetAndRegisterAtMaster();
            }
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
