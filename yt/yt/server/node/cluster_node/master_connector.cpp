#include "master_connector.h"

#include "private.h"
#include "bootstrap.h"
#include "config.h"
#include "dynamic_config_manager.h"
#include "node_resource_manager.h"

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/chunk_store.h>
#include <yt/yt/server/node/data_node/location.h>
#include <yt/yt/server/node/data_node/medium_directory_manager.h>
#include <yt/yt/server/node/data_node/medium_updater.h>
#include <yt/yt/server/node/data_node/network_statistics.h>
#include <yt/yt/server/node/data_node/session_manager.h>

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/chunk_cache.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>

#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/node_tracker_client/node_tracker_service_proxy.h>
#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/build/build.h>

namespace NYT::NClusterNode {

using namespace NApi;
using namespace NCellMasterClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDataNode;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

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

        const auto& connection = Bootstrap_->GetClient()->GetNativeConnection();
        const auto secondaryMasterCellTags = connection->GetSecondaryMasterCellTags();
        MasterCellTags_.insert(connection->GetPrimaryMasterCellTag());
        MasterCellTags_.insert(secondaryMasterCellTags.begin(), secondaryMasterCellTags.end());
        connection->GetMasterCellDirectory()->SubscribeCellDirectoryChanged(
            BIND(&TMasterConnector::OnMasterCellDirectoryChanged, MakeStrong(this))
                .Via(Bootstrap_->GetControlInvoker()));

        const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
        dynamicConfigManager->SubscribeConfigChanged(BIND(&TMasterConnector::OnDynamicConfigChanged, MakeWeak(this)));

        UpdateLocalHostName(/*useHostObjects*/ false);
    }

    void Start() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ResetAndRegisterAtMaster(/*firstTime*/ true);
    }

    TReqHeartbeat GetHeartbeatRequest()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(GetNodeId() != InvalidNodeId);

        TReqHeartbeat heartbeat;

        heartbeat.set_node_id(ToProto<ui32>(GetNodeId()));

        const auto& memoryTracker = Bootstrap_->GetMemoryUsageTracker();
        auto* protoMemory = heartbeat.mutable_statistics()->mutable_memory();
        protoMemory->set_total_limit(memoryTracker->GetTotalLimit());
        protoMemory->set_total_used(memoryTracker->GetTotalUsed());
        for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
            auto* protoCategory = protoMemory->add_categories();
            protoCategory->set_type(static_cast<int>(category));
            auto limit = memoryTracker->GetExplicitLimit(category);
            if (limit < std::numeric_limits<i64>::max()) {
                protoCategory->set_limit(limit);
            }
            auto used = memoryTracker->GetUsed(category);
            protoCategory->set_used(used);
        }

        Bootstrap_->GetNetworkStatistics().UpdateStatistics(heartbeat.mutable_statistics());

        const auto& resourceManager = Bootstrap_->GetNodeResourceManager();
        auto* protoCpu = heartbeat.mutable_statistics()->mutable_cpu();

        if (auto cpuLimit = resourceManager->GetCpuLimit()) {
            protoCpu->set_total_limit(*cpuLimit);
        }
        if (auto cpuGuarantee = resourceManager->GetCpuGuarantee()) {
            protoCpu->set_total_guarantee(*cpuGuarantee);
        }
        protoCpu->set_total_used(resourceManager->GetCpuUsage());
        protoCpu->set_jobs(resourceManager->GetJobsCpuLimit());
        protoCpu->set_tablet_slots(resourceManager->GetTabletSlotCpu());
        protoCpu->set_dedicated(resourceManager->GetNodeDedicatedCpu());

        ToProto(heartbeat.mutable_alerts(), GetAlerts());

        WaitFor(BIND(
            [this, &heartbeat, this_ = MakeStrong(this)] {
                const auto& jobResourceManager = Bootstrap_->GetJobResourceManager();
                *heartbeat.mutable_resource_limits() = ToNodeResources(jobResourceManager->GetResourceLimits());
                *heartbeat.mutable_resource_usage() = ToNodeResources(jobResourceManager->GetResourceUsage(/*includeWaiting*/ true));
            })
            .AsyncVia(Bootstrap_->GetJobInvoker())
            .Run())
        .ThrowOnError();

        return heartbeat;
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

    void OnHeartbeatResponse(const TRspHeartbeat& response)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto hostName = response.has_host_name() ? std::make_optional(response.host_name()) : std::nullopt;
        UpdateHostName(hostName);

        auto rack = response.has_rack() ? std::make_optional(response.rack()) : std::nullopt;
        UpdateRack(rack);

        auto dataCenter = response.has_data_center() ? std::make_optional(response.data_center()) : std::nullopt;
        UpdateDataCenter(dataCenter);

        auto tags = FromProto<std::vector<TString>>(response.tags());
        UpdateTags(std::move(tags));

        Bootstrap_->SetDecommissioned(response.decommissioned());

        const auto& resourceManager = Bootstrap_->GetNodeResourceManager();
        resourceManager->SetResourceLimitsOverride(response.resource_limits_overrides());

        const auto& jobResourceManager = Bootstrap_->GetJobResourceManager();
        jobResourceManager->SetResourceLimitsOverrides(response.resource_limits_overrides());
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
            ? TDuration::Zero()
            : *Config_->RegisterRetryPeriod + RandomDuration(*Config_->RegisterRetrySplay);

        TDelayedExecutor::Submit(
            BIND(&TMasterConnector::RegisterAtMaster, MakeStrong(this)),
            delay,
            MasterConnectionInvoker_);
    }

    NRpc::IChannelPtr GetMasterChannel(TCellTag cellTag) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto cellId = Bootstrap_->GetCellId(cellTag);
        const auto& client = Bootstrap_->GetClient();
        const auto& connection = client->GetNativeConnection();
        const auto& cellDirectory = connection->GetCellDirectory();
        return cellDirectory->GetChannelByCellId(cellId, NHydra::EPeerKind::Leader);
    }

    bool IsConnected() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return GetNodeId() != InvalidNodeId;
    }

    NNodeTrackerClient::TNodeId GetNodeId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return NodeId_.load();
    }

    TString GetLocalHostName() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return LocalHostName_.Load();
    }

    TMasterEpoch GetEpoch() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Epoch_.load();
    }

    const THashSet<TCellTag>& GetMasterCellTags() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return MasterCellTags_;
    }

private:
    NClusterNode::IBootstrap* const Bootstrap_;

    const TMasterConnectorConfigPtr Config_;

    const TAddressMap RpcAddresses_;
    const TAddressMap SkynetHttpAddresses_;
    const TAddressMap MonitoringHttpAddresses_;

    const std::vector<TString> NodeTags_;

    TCancelableContextPtr MasterConnectionContext_;

    IInvokerPtr MasterConnectionInvoker_;

    std::atomic<TNodeId> NodeId_ = InvalidNodeId;
    TAtomicObject<TString> LocalHostName_;

    std::atomic<TMasterEpoch> Epoch_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, LocalDescriptorLock_);
    NNodeTrackerClient::TNodeDescriptor LocalDescriptor_;

    NApi::ITransactionPtr LeaseTransaction_;

    TDuration HeartbeatPeriod_;
    TDuration HeartbeatPeriodSplay_;

    THashSet<TCellTag> MasterCellTags_;

    std::vector<TError> GetAlerts()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TError> alerts;
        PopulateAlerts_.Fire(&alerts);

        ExportAlerts(alerts);

        for (const auto& dynamicAlert : alerts) {
            YT_VERIFY(!dynamicAlert.IsOK());
            YT_LOG_WARNING(dynamicAlert, "Dynamic alert registered");
        }

        return alerts;
    }

    void ExportAlerts(const std::vector<TError>& alerts) const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        THashSet<int> codes;
        for (const auto& alert : alerts) {
            TraverseError(alert, [&codes] (const TError& error, int /*depth*/) {
                codes.insert(error.GetCode());
            });
        }

        TSensorBuffer buffer;
        for (const auto code : codes) {
            auto errorCodeInfo = TErrorCodeRegistry::Get()->Get(code);
            {
                TWithTagGuard guard(&buffer, "error_code", ToString(errorCodeInfo));
                buffer.AddGauge("/alerts", 1);
            }
        }

        Bootstrap_->GetBufferedProducer()->Update(std::move(buffer));
    }

    void UpdateHostName(const std::optional<TString>& hostName)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto guard = Guard(LocalDescriptorLock_);
        LocalDescriptor_ = NNodeTrackerClient::TNodeDescriptor(
            RpcAddresses_,
            hostName,
            LocalDescriptor_.GetRack(),
            LocalDescriptor_.GetDataCenter(),
            LocalDescriptor_.GetTags());
    }

    void UpdateRack(const std::optional<TString>& rack)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto guard = Guard(LocalDescriptorLock_);
        LocalDescriptor_ = NNodeTrackerClient::TNodeDescriptor(
            RpcAddresses_,
            LocalDescriptor_.GetHost(),
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
            LocalDescriptor_.GetHost(),
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
            LocalDescriptor_.GetHost(),
            LocalDescriptor_.GetRack(),
            LocalDescriptor_.GetDataCenter(),
            std::move(tags));
    }

    void OnLeaseTransactionAborted(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_WARNING(error, "Master transaction lease aborted");

        ResetAndRegisterAtMaster(/*firstTime*/ false);
    }

    void Reset()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (MasterConnectionContext_) {
            MasterConnectionContext_->Cancel(TError("Master disconnected"));
        }

        MasterConnectionContext_ = New<TCancelableContext>();
        MasterConnectionInvoker_ = MasterConnectionContext_->CreateInvoker(Bootstrap_->GetControlInvoker());

        NodeId_.store(InvalidNodeId);
        Epoch_++;

        MasterDisconnected_.Fire();
    }

    void RegisterAtMaster()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        try {
            StartLeaseTransaction();
            RegisterAtPrimaryMaster();
            // NB: InitMedia waiting for medium directory synchronization so we want to call it as late as possible.
            InitMedia();
            if (*Config_->SyncDirectoriesOnConnect) {
                SyncDirectories();
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error registering at primary master");
            ResetAndRegisterAtMaster(/*firstTime*/ false);
            return;
        }

        MasterConnected_.Fire(GetNodeId());

        YT_LOG_INFO("Successfully registered at primary master (NodeId: %v)",
            GetNodeId());

        StartHeartbeats();
    }

    void InitMedia()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // NB: Media initialization for data node occurred at registration at primary master.

        if (Bootstrap_->IsExecNode()) {
            const auto& nativeConnection = Bootstrap_->GetClient()->GetNativeConnection();
            WaitFor(nativeConnection->GetMediumDirectorySynchronizer()->RecentSync())
                .ThrowOnError();
        }
    }

    void StartLeaseTransaction()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& connection = Bootstrap_->GetClient()->GetNativeConnection();
        // NB: Node lease transaction is not Cypress to avoid chicken and egg problem with
        // Sequoia in future.
        TTransactionStartOptions options{
            .Timeout = Config_->LeaseTransactionTimeout,
            .PingPeriod = Config_->LeaseTransactionPingPeriod,
            .SuppressStartTimestampGeneration = true,
            .CoordinatorMasterCellTag = connection->GetPrimaryMasterCellTag(),
            .ReplicateToMasterCellTags = connection->GetSecondaryMasterCellTags(),
            .StartCypressTransaction = false,
        };

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Lease for node %v", GetDefaultAddress(RpcAddresses_)));
        options.Attributes = std::move(attributes);

        auto asyncTransaction = Bootstrap_->GetClient()->StartTransaction(ETransactionType::Master, options);
        LeaseTransaction_ = WaitFor(asyncTransaction)
            .ValueOrThrow();

        LeaseTransaction_->SubscribeAborted(
            BIND(&TMasterConnector::OnLeaseTransactionAborted, MakeWeak(this))
                .Via(MasterConnectionInvoker_));
    }

    void RegisterAtPrimaryMaster()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto masterChannel = GetMasterChannel(PrimaryMasterCellTagSentinel);
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
        if (auto hostName = Bootstrap_->GetConfig()->HostName) {
            req->set_host_name(hostName);
        }

        for (auto flavor : Bootstrap_->GetFlavors()) {
            req->add_flavors(static_cast<int>(flavor));
        }

        req->set_cypress_annotations(ConvertToYsonString(Bootstrap_->GetConfig()->CypressAnnotations).ToString());
        req->set_build_version(GetVersion());

        req->set_exec_node_is_not_data_node(Bootstrap_->GetConfig()->ExecNodeIsNotDataNode);

        req->set_chunk_locations_supported(true);

        if (Bootstrap_->NeedDataNodeBootstrap()) {
            const auto& storeLocations = Bootstrap_
                ->GetDataNodeBootstrap()
                ->GetChunkStore()
                ->Locations();
            for (const auto& location : storeLocations) {
                if (location->CanPublish()) {
                    ToProto(req->add_chunk_location_uuids(), location->GetUuid());
                }
            }
            // COMPAT(kvk1920)
            req->set_location_directory_supported(true);
        }

        auto tableMountConfig = New<NTabletNode::TTableMountConfig>();
        ToProto(req->mutable_table_mount_config_keys(), tableMountConfig->GetRegisteredKeys());

        YT_LOG_INFO("Registering at primary master");

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        // COMPAT(pogorelov): Remove when all masters will be 24.1.
        if (rsp->tags_size() > 0) {
            auto tags = FromProto<std::vector<TString>>(rsp->tags());
            UpdateTags(std::move(tags));
        }

        Bootstrap_->CompleteNodeRegistration();

        if (Bootstrap_->NeedDataNodeBootstrap()) {
            const auto& dataNodeBootstrap = Bootstrap_->GetDataNodeBootstrap();
            const auto& mediumUpdater = dataNodeBootstrap->GetMediumUpdater();
            if (rsp->HasExtension(TDataNodeInfoExt::data_node_info_ext)) {
                const auto& dataNodeInfoExt = rsp->GetExtension(TDataNodeInfoExt::data_node_info_ext);

                YT_VERIFY(dataNodeInfoExt.has_medium_directory());
                const auto& mediumDirectoryManager = dataNodeBootstrap->GetMediumDirectoryManager();
                mediumDirectoryManager->UpdateMediumDirectory(dataNodeInfoExt.medium_directory());

                YT_VERIFY(dataNodeInfoExt.has_medium_overrides());
                mediumUpdater->UpdateLocationMedia(dataNodeInfoExt.medium_overrides(), /*onInitialize*/ true);

                dataNodeBootstrap->SetLocationUuidsRequired(dataNodeInfoExt.require_location_uuids());
            } else {
                dataNodeBootstrap->SetLocationUuidsRequired(true);
                mediumUpdater->UpdateLocationMedia({}, /*onInitialize*/ true);
            }
        }

        NodeId_.store(FromProto<TNodeId>(rsp->node_id()));
    }

    void SyncDirectories()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& connection = Bootstrap_->GetClient()->GetNativeConnection();

        YT_LOG_INFO("Synchronizing cell directory");
        WaitFor(connection->GetCellDirectorySynchronizer()->Sync())
            .ThrowOnError();
        YT_LOG_INFO("Cell directory synchronized");

        YT_LOG_INFO("Synchronizing cluster directory");
        WaitFor(connection->GetClusterDirectorySynchronizer()->Sync())
            .ThrowOnError();
        YT_LOG_INFO("Cluster directory synchronized");
        // TODO(cherepashka): add synchronization of master cell directory in future.
    }

    void StartHeartbeats()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Start reporting cluster node heartbeats to master");
        ScheduleHeartbeat(/*immediately*/ true);
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

        YT_VERIFY(GetNodeId() != InvalidNodeId);

        // Cluster node heartbeats are required at primary master only.
        auto masterChannel = GetMasterChannel(PrimaryMasterCellTagSentinel);
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
            ScheduleHeartbeat(/*immediately*/ false);
        } else {
            YT_LOG_WARNING(rspOrError, "Error reporting cluster node heartbeat to master");
            if (IsRetriableError(rspOrError)) {
                ScheduleHeartbeat(/* immediately*/ false);
            } else {
                ResetAndRegisterAtMaster(/*firstTime*/ false);
            }
        }
    }

    void UpdateLocalHostName(bool useHostObjects)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (useHostObjects) {
            auto hostName = GetLocalDescriptor().GetHost().value_or(Bootstrap_->GetConfig()->HostName);
            if (hostName.empty()) {
                hostName = NNet::GetLocalHostName();
            }
            LocalHostName_.Store(hostName);
        } else {
            LocalHostName_.Store(NNet::GetLocalHostName());
        }
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        HeartbeatPeriod_ = newNodeConfig->MasterConnector->HeartbeatPeriod.value_or(Config_->HeartbeatPeriod);
        HeartbeatPeriodSplay_ = newNodeConfig->MasterConnector->HeartbeatPeriodSplay.value_or(Config_->HeartbeatPeriodSplay);

        UpdateLocalHostName(newNodeConfig->MasterConnector->UseHostObjects);
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
