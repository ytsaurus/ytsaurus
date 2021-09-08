#include "master_connector.h"

#include "private.h"
#include "bootstrap.h"
#include "config.h"
#include "dynamic_config_manager.h"
#include "node_resource_manager.h"

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/chunk_store.h>
#include <yt/yt/server/node/data_node/location.h>
#include <yt/yt/server/node/data_node/legacy_master_connector.h>
#include <yt/yt/server/node/data_node/network_statistics.h>
#include <yt/yt/server/node/data_node/session_manager.h>

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/chunk_cache.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>

#include <yt/yt/server/node/job_agent/job_controller.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/build/build.h>

namespace NYT::NClusterNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDataNode;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NYTree;

using NNodeTrackerClient::TAddressMap;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ClusterNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TMasterConnector
    : public IMasterConnector
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(std::vector<TError>* alerts), PopulateAlerts);

    DEFINE_SIGNAL_OVERRIDE(void(NNodeTrackerClient::TNodeId nodeId), MasterConnected);

    DEFINE_SIGNAL_OVERRIDE(void(), MasterDisconnected);

public:
    TMasterConnector(
        IBootstrap* bootstrap,
        const TAddressMap& rpcAddresses,
        const TAddressMap& skynetHttpAddresses,
        const TAddressMap& monitoringHttpAddresses,
        const std::vector<TString>& nodeTags)
        : Bootstrap_(bootstrap)
        , Config_(Bootstrap_->GetConfig()->MasterConnector)
        , RpcAddresses_(rpcAddresses)
        , SkynetHttpAddresses_(skynetHttpAddresses)
        , MonitoringHttpAddresses_(monitoringHttpAddresses)
        , NodeTags_(nodeTags)
        , LocalDescriptor_(RpcAddresses_)
        , HeartbeatPeriod_(Config_->HeartbeatPeriod)
        , HeartbeatPeriodSplay_(Config_->HeartbeatPeriodSplay)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
    }

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& connection = Bootstrap_->GetMasterClient()->GetNativeConnection();
        MasterCellTags_.push_back(connection->GetPrimaryMasterCellTag());
        for (auto cellTag : connection->GetSecondaryMasterCellTags()) {
            MasterCellTags_.push_back(cellTag);
        }

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TMasterConnector::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void Start() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ResetAndRegisterAtMaster(/* firstTime */ true);
    }

    TReqHeartbeat GetHeartbeatRequest() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(GetNodeId() != InvalidNodeId);

        TReqHeartbeat heartbeat;

        heartbeat.set_node_id(GetNodeId());

        const auto& tracker = Bootstrap_->GetMemoryUsageTracker();
        auto* protoMemory = heartbeat.mutable_statistics()->mutable_memory();
        protoMemory->set_total_limit(tracker->GetTotalLimit());
        protoMemory->set_total_used(tracker->GetTotalUsed());
        for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
            auto* protoCategory = protoMemory->add_categories();
            protoCategory->set_type(static_cast<int>(category));
            auto limit = tracker->GetExplicitLimit(category);
            if (limit < std::numeric_limits<i64>::max()) {
                protoCategory->set_limit(limit);
            }
            auto used = tracker->GetUsed(category);
            protoCategory->set_used(used);
        }

        Bootstrap_->GetNetworkStatistics().UpdateStatistics(heartbeat.mutable_statistics());

        ToProto(heartbeat.mutable_alerts(), GetAlerts());

        return heartbeat;
    }

    void OnHeartbeatResponse(const TRspHeartbeat& response) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto rack = response.has_rack() ? std::make_optional(response.rack()) : std::nullopt;
        UpdateRack(rack);

        auto dataCenter = response.has_data_center() ? std::make_optional(response.data_center()) : std::nullopt;
        UpdateDataCenter(dataCenter);

        auto tags = FromProto<std::vector<TString>>(response.tags());
        UpdateTags(std::move(tags));

        Bootstrap_->SetDecommissioned(response.decommissioned());

        const auto& resourceManager = Bootstrap_->GetNodeResourceManager();
        resourceManager->SetResourceLimitsOverride(response.resource_limits_overrides());

        const auto& jobController = Bootstrap_->GetJobController();
        jobController->SetResourceLimitsOverrides(response.resource_limits_overrides());
    }

    NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(LocalDescriptorLock_);
        return LocalDescriptor_;
    }

    const IInvokerPtr& GetMasterConnectionInvoker() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return MasterConnectionInvoker_;
    }

    void ResetAndRegisterAtMaster(bool firstTime) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Reset();

        auto delay = firstTime
            ? RandomDuration(*Config_->FirstRegisterSplay)
            : *Config_->RegisterRetryPeriod + RandomDuration(*Config_->RegisterRetrySplay);

        TDelayedExecutor::Submit(
            BIND(&TMasterConnector::RegisterAtMaster, MakeStrong(this)),
            delay,
            MasterConnectionInvoker_);
    }

    NRpc::IChannelPtr GetMasterChannel(TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto cellId = Bootstrap_->GetCellId(cellTag);
        const auto& client = Bootstrap_->GetMasterClient();
        const auto& connection = client->GetNativeConnection();
        const auto& cellDirectory = connection->GetCellDirectory();
        return cellDirectory->GetChannel(cellId, NHydra::EPeerKind::Leader);
    }

    bool IsConnected() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return NodeId_ != InvalidNodeId;
    }

    NNodeTrackerClient::TNodeId GetNodeId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return NodeId_.load();
    }

    bool UseNewHeartbeats() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_VERIFY(UseNewHeartbeats_);
        return *UseNewHeartbeats_;
    }

    const NObjectClient::TCellTagList& GetMasterCellTags() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return MasterCellTags_;
    }

private:
    NClusterNode::IBootstrap* const Bootstrap_;

    const TMasterConnectorConfigPtr Config_;

    std::optional<bool> UseNewHeartbeats_;

    const TAddressMap RpcAddresses_;
    const TAddressMap SkynetHttpAddresses_;
    const TAddressMap MonitoringHttpAddresses_;

    const std::vector<TString> NodeTags_;

    TCancelableContextPtr MasterConnectionContext_;

    IInvokerPtr MasterConnectionInvoker_;

    std::atomic<TNodeId> NodeId_ = InvalidNodeId;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, LocalDescriptorLock_);
    NNodeTrackerClient::TNodeDescriptor LocalDescriptor_;

    NApi::ITransactionPtr LeaseTransaction_;

    TDuration HeartbeatPeriod_;
    TDuration HeartbeatPeriodSplay_;

    TCellTagList MasterCellTags_;

    std::vector<TError> GetAlerts()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TError> alerts;
        PopulateAlerts_.Fire(&alerts);

        for (const auto& dynamicAlert : alerts) {
            YT_VERIFY(!dynamicAlert.IsOK());
            YT_LOG_WARNING(dynamicAlert, "Dynamic alert registered");
        }

        return alerts;
    }

    void UpdateRack(const std::optional<TString>& rack)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto guard = Guard(LocalDescriptorLock_);
        LocalDescriptor_ = NNodeTrackerClient::TNodeDescriptor(
            RpcAddresses_,
            rack,
            LocalDescriptor_.GetDataCenter(),
            LocalDescriptor_.GetTags());
    }

    void UpdateDataCenter(const std::optional<TString>& dc)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto guard = Guard(LocalDescriptorLock_);
        LocalDescriptor_ = NNodeTrackerClient::TNodeDescriptor(
            RpcAddresses_,
            LocalDescriptor_.GetRack(),
            dc,
            LocalDescriptor_.GetTags());
    }

    void UpdateTags(std::vector<TString> tags)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto guard = Guard(LocalDescriptorLock_);
        LocalDescriptor_ = NNodeTrackerClient::TNodeDescriptor(
            RpcAddresses_,
            LocalDescriptor_.GetRack(),
            LocalDescriptor_.GetDataCenter(),
            std::move(tags));
    }

    void OnLeaseTransactionAborted(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_WARNING(error, "Master transaction lease aborted");

        ResetAndRegisterAtMaster(/* firstTime */ false);
    }

    void Reset()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (MasterConnectionContext_) {
            MasterConnectionContext_->Cancel(TError("Master disconnceted"));
        }

        MasterConnectionContext_ = New<TCancelableContext>();
        MasterConnectionInvoker_ = MasterConnectionContext_->CreateInvoker(Bootstrap_->GetControlInvoker());

        NodeId_.store(InvalidNodeId);

        Bootstrap_->GetLegacyMasterConnector()->Reset();

        MasterDisconnected_.Fire();
    }

    void RegisterAtMaster()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        try {
            InitMedia();
            StartLeaseTransaction();
            RegisterAtPrimaryMaster();
            if (*Config_->SyncDirectoriesOnConnect) {
                SyncDirectories();
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error registering at primary master");
            ResetAndRegisterAtMaster(/* firstTime */ false);
            return;
        }

        MasterConnected_.Fire(GetNodeId());

        YT_LOG_INFO("Successfully registered at primary master (NodeId: %v)",
            GetNodeId());

        if (UseNewHeartbeats()) {
            StartHeartbeats();
        } else {
            Bootstrap_->GetLegacyMasterConnector()->OnMasterConnected();
        }
    }

    void InitMedia()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        WaitFor(Bootstrap_
            ->GetMasterClient()
            ->GetNativeConnection()
            ->GetMediumDirectorySynchronizer()
            ->RecentSync())
            .ThrowOnError();

        if (Bootstrap_->IsDataNode()) {
            const auto& storeLocations = Bootstrap_
                ->GetDataNodeBootstrap()
                ->GetChunkStore()
                ->Locations();
            for (const auto& location : storeLocations) {
                location->UpdateMediumName(location->GetMediumName());
            }
        }

        if (Bootstrap_->IsExecNode()) {
            const auto& cacheLocations = Bootstrap_
                ->GetExecNodeBootstrap()
                ->GetChunkCache()
                ->Locations();
            for (const auto& location : cacheLocations) {
                location->UpdateMediumName(location->GetMediumName());
            }
        }

        auto mediumDirectory = Bootstrap_
            ->GetMasterClient()
            ->GetNativeConnection()
            ->GetMediumDirectory();

        if (Bootstrap_->IsExecNode()) {
            Bootstrap_
                ->GetExecNodeBootstrap()
                ->GetSlotManager()
                ->InitMedia(mediumDirectory);
        }
    }

    void StartLeaseTransaction()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& connection = Bootstrap_->GetMasterClient()->GetNativeConnection();
        TTransactionStartOptions options{
            .Timeout = Config_->LeaseTransactionTimeout,
            .PingPeriod = Config_->LeaseTransactionPingPeriod,
            .SuppressStartTimestampGeneration = true,
            .CoordinatorMasterCellTag = connection->GetPrimaryMasterCellTag(),
            .ReplicateToMasterCellTags = connection->GetSecondaryMasterCellTags()
        };

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Lease for node %v", GetDefaultAddress(RpcAddresses_)));
        options.Attributes = std::move(attributes);

        auto asyncTransaction = Bootstrap_->GetMasterClient()->StartTransaction(ETransactionType::Master, options);
        LeaseTransaction_ = WaitFor(asyncTransaction)
            .ValueOrThrow();

        LeaseTransaction_->SubscribeAborted(
            BIND(&TMasterConnector::OnLeaseTransactionAborted, MakeWeak(this))
                .Via(MasterConnectionInvoker_));
    }

    void RegisterAtPrimaryMaster()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto masterChannel = GetMasterChannel(PrimaryMasterCellTag);
        TNodeTrackerServiceProxy proxy(masterChannel);

        auto req = proxy.RegisterNode();
        req->SetTimeout(Config_->RegisterTimeout);

        auto* nodeAddresses = req->mutable_node_addresses();

        auto* rpcAddresses = nodeAddresses->add_entries();
        rpcAddresses->set_address_type(static_cast<int>(EAddressType::InternalRpc));
        ToProto(rpcAddresses->mutable_addresses(), RpcAddresses_);

        auto* skynetHttpAddresses = nodeAddresses->add_entries();
        skynetHttpAddresses->set_address_type(static_cast<int>(EAddressType::SkynetHttp));
        ToProto(skynetHttpAddresses->mutable_addresses(), SkynetHttpAddresses_);

        auto* monitoringHttpAddresses = nodeAddresses->add_entries();
        monitoringHttpAddresses->set_address_type(static_cast<int>(EAddressType::MonitoringHttp));
        ToProto(monitoringHttpAddresses->mutable_addresses(), MonitoringHttpAddresses_);

        ToProto(req->mutable_lease_transaction_id(), LeaseTransaction_->GetId());
        ToProto(req->mutable_tags(), NodeTags_);

        for (auto flavor : Bootstrap_->GetFlavors()) {
            req->add_flavors(static_cast<int>(flavor));
        }

        req->set_cypress_annotations(ConvertToYsonString(Bootstrap_->GetConfig()->CypressAnnotations).ToString());
        req->set_build_version(GetVersion());

        if (Bootstrap_->IsDataNode()) {
            const auto& storeLocations = Bootstrap_
                ->GetDataNodeBootstrap()
                ->GetChunkStore()
                ->Locations();
            for (const auto& location : storeLocations) {
                if (location->IsEnabled()) {
                    ToProto(req->add_location_uuids(), location->GetUuid());
                }
            }
        }

        YT_LOG_INFO("Registering at primary master");

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        // Changing protocol without restart does not seem safe and is not required.
        if (!UseNewHeartbeats_) {
            UseNewHeartbeats_ = Bootstrap_->GetConfig()->UseNewHeartbeats && rsp->use_new_heartbeats();
            if (*UseNewHeartbeats_) {
                YT_LOG_INFO("Using new heartbeats");
            } else {
                YT_LOG_INFO("Using old heartbeats");
            }
        }

        NodeId_.store(rsp->node_id());
    }

    void SyncDirectories()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& connection = Bootstrap_->GetMasterClient()->GetNativeConnection();

        YT_LOG_INFO("Synchronizing cell directory");
        WaitFor(connection->GetCellDirectorySynchronizer()->Sync())
            .ThrowOnError();
        YT_LOG_INFO("Cell directory synchronized");

        YT_LOG_INFO("Synchronizing cluster directory");
        WaitFor(connection->GetClusterDirectorySynchronizer()->Sync())
            .ThrowOnError();
        YT_LOG_INFO("Cluster directory synchronized");
    }

    void StartHeartbeats()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Start reporting cluster node heartbeats to master");
        ScheduleHeartbeat(/* immediately */ true);
    }

    void ScheduleHeartbeat(bool immediately)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto delay = immediately ? TDuration::Zero() : HeartbeatPeriod_ + RandomDuration(HeartbeatPeriodSplay_);
        TDelayedExecutor::Submit(
            BIND(&TMasterConnector::ReportHeartbeat, MakeWeak(this)),
            delay,
            MasterConnectionInvoker_);
    }

    void ReportHeartbeat()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Cluster node heartbeats are required at primary master only.
        auto masterChannel = GetMasterChannel(PrimaryMasterCellTag);
        TNodeTrackerServiceProxy proxy(masterChannel);

        auto req = proxy.Heartbeat();
        req->SetTimeout(Config_->HeartbeatTimeout);

        static_cast<TReqHeartbeat&>(*req) = GetHeartbeatRequest();

        YT_LOG_INFO("Sending cluster node heartbeat to master (%v)",
            req->statistics());

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.IsOK()) {
            OnHeartbeatResponse(*rspOrError.Value());

            YT_LOG_INFO("Successfully reported cluster node heartbeat to master");

            // Schedule next heartbeat.
            ScheduleHeartbeat(/* immediately */ false);
        } else {
            YT_LOG_WARNING(rspOrError, "Error reporting cluster node heartbeat to master");
            if (IsRetriableError(rspOrError)) {
                ScheduleHeartbeat(/* immediately*/ false);
            } else {
                ResetAndRegisterAtMaster(/* firstTime */ false);
            }
        }
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        HeartbeatPeriod_ = newNodeConfig->MasterConnector->HeartbeatPeriod.value_or(Config_->HeartbeatPeriod);
        HeartbeatPeriodSplay_ = newNodeConfig->MasterConnector->HeartbeatPeriodSplay.value_or(Config_->HeartbeatPeriodSplay);
    }

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(
    IBootstrap* bootstrap,
    const NNodeTrackerClient::TAddressMap& rpcAddresses,
    const NNodeTrackerClient::TAddressMap& skynetHttpAddresses,
    const NNodeTrackerClient::TAddressMap& monitoringHttpAddresses,
    const std::vector<TString>& nodeTags)
{
    return New<TMasterConnector>(
        bootstrap,
        rpcAddresses,
        skynetHttpAddresses,
        monitoringHttpAddresses,
        nodeTags);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
