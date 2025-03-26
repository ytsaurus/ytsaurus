#include "bootstrap.h"

#include "chunk_cache.h"
#include "controller_agent_connector.h"
#include "exec_node_admin_service.h"
#include "gpu_manager.h"
#include "job_controller.h"
#include "job_prober_service.h"
#include "job_proxy_log_manager.h"
#include "master_connector.h"
#include "orchid.h"
#include "private.h"
#include "scheduler_connector.h"
#include "slot_manager.h"
#include "supervisor_service.h"
#include "throttler_manager.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/exec_node/job_input_cache.h>
#include <yt/yt/server/node/exec_node/proxying_data_node_service.h>

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/ytree_integration.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/misc/job_reporter.h>

#include <yt/yt/server/lib/nbd/config.h>
#include <yt/yt/server/lib/nbd/server.h>

#include <yt/yt/ytlib/auth/native_authentication_manager.h>
#include <yt/yt/ytlib/auth/tvm_bridge_service.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

#include <yt/yt/library/dns_over_rpc/server/dns_over_rpc_service.h>

#include <yt/yt/library/disk_manager/hotswap_manager.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/service_discovery/yp/config.h>

#include <yt/yt/core/logging/config.h>

namespace NYT::NExecNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NDataNode;
using namespace NJobAgent;
using namespace NJobProxy;
using namespace NNodeTrackerClient;
using namespace NProfiling;
using namespace NYTree;
using namespace NScheduler;
using namespace NServer;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
    , public TBootstrapBase
{
public:
    explicit TBootstrap(NClusterNode::IBootstrap* bootstrap)
        : TBootstrapBase(bootstrap)
        , ClusterNodeBootstrap_(bootstrap)
    { }

    void Initialize() override
    {
        YT_LOG_INFO("Initializing exec node");

        // Cycles are fine for bootstrap.
        GetDynamicConfigManager()
            ->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnDynamicConfigChanged, MakeStrong(this)));

        SlotManager_ = New<TSlotManager>(this);

        GpuManager_ = New<TGpuManager>(this);

        JobReporter_ = New<TJobReporter>(
            New<TJobReporterConfig>(),
            GetConnection());

        MasterConnector_ = CreateMasterConnector(this);

        SchedulerConnector_ = New<TSchedulerConnector>(this);

        // We must ensure we know actual status of job proxy binary before Run phase.
        // Otherwise we may erroneously receive some job which we fail to run due to missing
        // ytserver-job-proxy. This requires slot manager to be initialized before job controller
        // in order for the first out-of-band job proxy build info update to reach job controller
        // via signal.
        JobController_ = CreateJobController(this);

        // COMPAT(pogorelov)
        if (GetConfig()->ExecNode->JobProxy->JobProxyLogging->Mode == EJobProxyLoggingMode::PerJobDirectory) {
            JobProxyLogManager_ = CreateJobProxyLogManager(this);
        }

        ControllerAgentConnectorPool_ = New<TControllerAgentConnectorPool>(this);

        BuildJobProxyConfigTemplate();

        ChunkCache_ = New<TChunkCache>(GetConfig()->DataNode, this);

        DynamicConfig_.Store(New<TClusterNodeDynamicConfig>());

        JobProxySolomonExporter_ = New<TSolomonExporter>(
            GetConfig()->ExecNode->JobProxySolomonExporter,
            New<TSolomonRegistry>());

        ThrottlerManager_ = CreateThrottlerManager(
            ClusterNodeBootstrap_,
            {
                .LocalAddress = NNet::BuildServiceAddress(GetLocalHostName(), GetConfig()->RpcPort),
                .Logger = ExecNodeLogger().WithTag("Component: ThrottlerManager"),
                .Profiler = ExecNodeProfiler().WithPrefix("/throttler_manager")
            }
        );

        JobInputCache_ = CreateJobInputCache(this);

        GetRpcServer()->RegisterService(CreateProxyingDataNodeService(this));

        GetRpcServer()->RegisterService(CreateJobProberService(this));

        GetRpcServer()->RegisterService(CreateSupervisorService(this));

        if (auto tvmService = NAuth::TNativeAuthenticationManager::Get()->GetTvmService()) {
            // TODO(gepardo): Allow this service only for local requests from job proxy. See YT-17806 for details.
            GetRpcServer()->RegisterService(CreateTvmBridgeService(
                tvmService,
                NRpc::TDispatcher::Get()->GetLightInvoker(),
                /*authenticator*/ nullptr));
        }

        GetRpcServer()->RegisterService(CreateExecNodeAdminService(this));

        GetRpcServer()->RegisterService(NDns::CreateDnsOverRpcService(
            NNet::TAddressResolver::Get()->GetDnsResolver(),
            DnsOverRpcActionQueue_->GetInvoker()));

        // NB(psushin): initialize chunk cache first because slot manager (and root
        // volume manager inside it) can start using it to populate tmpfs layers cache.
        ChunkCache_->Initialize();
        // COMPAT(pogorelov)
        if (JobProxyLogManager_) {
            JobProxyLogManager_->Initialize();
        }
        SlotManager_->Initialize();
        JobController_->Initialize();
        MasterConnector_->Initialize();
        SchedulerConnector_->Initialize();
        ControllerAgentConnectorPool_->Initialize();

        if (auto hotswapManager = ClusterNodeBootstrap_->TryGetHotswapManager()) {
            SubscribePopulateAlerts(BIND_NO_PROPAGATE(&NDiskManager::IHotswapManager::PopulateAlerts, hotswapManager));
        }
    }

    void Run() override
    {
        {
            YT_LOG_INFO("Waiting for throttlers to initialize");
            auto error = WaitFor(ThrottlerManager_->Start());
            YT_LOG_FATAL_UNLESS(
                error.IsOK(),
                error,
                "Unexpected failure while waiting for throttlers to initialize");
            YT_LOG_INFO("Throttlers initialized");
        }

        auto nbdConfig = DynamicConfig_.Acquire()->ExecNode->Nbd;
        if (nbdConfig && nbdConfig->Enabled) {
            // Create NBD server.
            NbdThreadPool_ = CreateThreadPool(nbdConfig->Server->ThreadCount, "Nbd", { .ThreadPriority = EThreadPriority::RealTime });
            NbdServer_ = CreateNbdServer(
                nbdConfig->Server,
                NBus::TTcpDispatcher::Get()->GetXferPoller(),
                NbdThreadPool_->GetInvoker());

            // Create block caches to read from Cypress.
            NApi::NNative::TConnectionOptions connectionOptions;
            connectionOptions.ConnectionInvoker = NbdThreadPool_->GetInvoker();
            auto connection = CreateConnection(TBootstrapBase::GetConnection()->GetCompoundConfig(), std::move(connectionOptions));
            connection->GetNodeDirectorySynchronizer()->Start();
            connection->GetClusterDirectorySynchronizer()->Start();

            auto clientOptions = NYT::NApi::TClientOptions::FromUser(NSecurityClient::RootUserName);
            auto client = connection->CreateNativeClient(clientOptions);

            auto blockCacheConfig = New<TBlockCacheConfig>();
            blockCacheConfig->CompressedData->Capacity = nbdConfig->BlockCacheCompressedDataCapacity;

            auto layerBlockCache = CreateClientBlockCache(
                blockCacheConfig,
                EBlockType::CompressedData,
                GetNullMemoryUsageTracker());

            LayerReaderHost_ = New<TChunkReaderHost>(
                client,
                /*localDescriptor*/ NNodeTrackerClient::TNodeDescriptor{},
                std::move(layerBlockCache),
                connection->GetChunkMetaCache(),
                /*nodeStatusDirectory*/ nullptr,
                /*bandwidthThrottler*/ GetUnlimitedThrottler(),
                /*rpsThrottler*/ GetUnlimitedThrottler(),
                /*mediumThrottler*/ GetUnlimitedThrottler(),
                /*trafficMeter*/ nullptr);

            auto fileBlockCache = CreateClientBlockCache(
                std::move(blockCacheConfig),
                EBlockType::CompressedData,
                GetNullMemoryUsageTracker());

            FileReaderHost_ = New<TChunkReaderHost>(
                client,
                /*localDescriptor*/ NNodeTrackerClient::TNodeDescriptor{},
                std::move(fileBlockCache),
                connection->GetChunkMetaCache(),
                /*nodeStatusDirectory*/ nullptr,
                /*bandwidthThrottler*/ GetUnlimitedThrottler(),
                /*rpsThrottler*/ GetUnlimitedThrottler(),
                /*mediumThrottler*/ GetUnlimitedThrottler(),
                /*trafficMeter*/ nullptr);
        }

        SetNodeByYPath(
            GetOrchidRoot(),
            "/exec_node",
            CreateVirtualNode(GetOrchidService(this)));

        if (auto hotswapManager = ClusterNodeBootstrap_->TryGetHotswapManager()) {
            SetNodeByYPath(
                GetOrchidRoot(),
                "/disk_monitoring",
                CreateVirtualNode(hotswapManager->GetOrchidService()));
        }

        // COMPAT(pogorelov)
        if (JobProxyLogManager_) {
            JobProxyLogManager_->Start();
        }

        SlotManager_->Start();
        JobController_->Start();

        JobProxySolomonExporter_->Register("/solomon/job_proxy", GetHttpServer());
        JobProxySolomonExporter_->Start();

        SchedulerConnector_->Start();

        ControllerAgentConnectorPool_->Start();
    }

    const IJobInputCachePtr& GetJobInputCache() const override
    {
        return JobInputCache_;
    }

    const TGpuManagerPtr& GetGpuManager() const override
    {
        return GpuManager_;
    }

    const TSlotManagerPtr& GetSlotManager() const override
    {
        return SlotManager_;
    }

    const TJobReporterPtr& GetJobReporter() const override
    {
        return JobReporter_;
    }

    const TJobProxyInternalConfigPtr& GetJobProxyConfigTemplate() const override
    {
        return JobProxyConfigTemplate_;
    }

    const TChunkCachePtr& GetChunkCache() const override
    {
        return ChunkCache_;
    }

    bool IsSimpleEnvironment() const override
    {
        return GetJobEnvironmentType() == EJobEnvironmentType::Simple;
    }

    const IJobControllerPtr& GetJobController() const override
    {
        return JobController_;
    }

    const IMasterConnectorPtr& GetMasterConnector() const override
    {
        return MasterConnector_;
    }

    const TSchedulerConnectorPtr& GetSchedulerConnector() const override
    {
        return SchedulerConnector_;
    }

    IThroughputThrottlerPtr GetThrottler(
        EExecNodeThrottlerKind kind,
        EThrottlerTrafficType trafficType,
        std::optional<TClusterName> remoteClusterName) const override
    {
        return ThrottlerManager_->GetOrCreateThrottler(kind, trafficType, std::move(remoteClusterName));
    }

    const TSolomonExporterPtr& GetJobProxySolomonExporter() const override
    {
        return JobProxySolomonExporter_;
    }

    const TControllerAgentConnectorPoolPtr& GetControllerAgentConnectorPool() const override
    {
        return ControllerAgentConnectorPool_;
    }

    TClusterNodeDynamicConfigPtr GetDynamicConfig() const override
    {
        return DynamicConfig_.Acquire();
    }

    NYT::NNbd::INbdServerPtr GetNbdServer() const override
    {
        return NbdServer_;
    }

    TChunkReaderHostPtr GetFileReaderHost() const override
    {
        return FileReaderHost_;
    }

    TChunkReaderHostPtr GetLayerReaderHost() const override
    {
        return LayerReaderHost_;
    }

    const IJobProxyLogManagerPtr& GetJobProxyLogManager() const override
    {
        return JobProxyLogManager_;
    }

    IThrottlerManagerPtr GetThrottlerManager() const override
    {
        return ThrottlerManager_;
    }

    void UpdateNodeProfilingTags(std::vector<NProfiling::TTag> dynamicTags) const override
    {
        std::ranges::sort(dynamicTags);

        auto updateDynamicTags = [] (std::vector<NProfiling::TTag> dynamicTags, const TSolomonRegistryPtr& registry) {
            auto currentTags = registry->GetDynamicTags();
            std::ranges::sort(currentTags);

            if (currentTags != dynamicTags) {
                registry->SetDynamicTags(std::move(dynamicTags));
            }
        };

        updateDynamicTags(dynamicTags, TSolomonRegistry::Get());
        updateDynamicTags(std::move(dynamicTags), GetJobProxySolomonExporter()->GetRegistry());
    }

private:
    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;

    const TActionQueuePtr DnsOverRpcActionQueue_ = New<TActionQueue>("DnsOverRpc");

    IJobInputCachePtr JobInputCache_;

    TSlotManagerPtr SlotManager_;

    TGpuManagerPtr GpuManager_;

    TJobReporterPtr JobReporter_;

    TJobProxyInternalConfigPtr JobProxyConfigTemplate_;

    TChunkCachePtr ChunkCache_;

    IMasterConnectorPtr MasterConnector_;

    TSchedulerConnectorPtr SchedulerConnector_;

    IJobControllerPtr JobController_;

    TSolomonExporterPtr JobProxySolomonExporter_;

    IThrottlerManagerPtr ThrottlerManager_;

    TControllerAgentConnectorPoolPtr ControllerAgentConnectorPool_;

    //! TODO(arkady-e1ppa): Get rid of unneeded options in ExecNode config.
    //! then remove GetDynamicConfig method and make this non-atomic again.
    TAtomicIntrusivePtr<TClusterNodeDynamicConfig> DynamicConfig_;

    IThreadPoolPtr NbdThreadPool_;
    NNbd::INbdServerPtr NbdServer_;

    TChunkReaderHostPtr FileReaderHost_;
    TChunkReaderHostPtr LayerReaderHost_;

    IJobProxyLogManagerPtr JobProxyLogManager_;

    void BuildJobProxyConfigTemplate()
    {
        auto localAddress = NNet::BuildServiceAddress(NNet::GetLoopbackAddress(), GetConfig()->RpcPort);

        JobProxyConfigTemplate_ = New<NJobProxy::TJobProxyInternalConfig>();

        auto singletonsConfig = TSingletonManager::GetConfig();
        JobProxyConfigTemplate_->SetSingletonConfig(singletonsConfig->GetSingletonConfig<TFiberManagerConfig>());

        {
            auto config = CloneYsonStruct(singletonsConfig->GetSingletonConfig<NNet::TAddressResolverConfig>());
            config->LocalHostNameOverride = NNet::GetLocalHostName();
            JobProxyConfigTemplate_->SetSingletonConfig(std::move(config));
        }

        JobProxyConfigTemplate_->SetSingletonConfig(singletonsConfig->GetSingletonConfig<NRpc::TDispatcherConfig>());
        JobProxyConfigTemplate_->SetSingletonConfig(singletonsConfig->GetSingletonConfig<NBus::TTcpDispatcherConfig>());
        JobProxyConfigTemplate_->SetSingletonConfig(singletonsConfig->TryGetSingletonConfig<NServiceDiscovery::NYP::TServiceDiscoveryConfig>());
        JobProxyConfigTemplate_->SetSingletonConfig(singletonsConfig->GetSingletonConfig<NChunkClient::TDispatcherConfig>());
        JobProxyConfigTemplate_->SetSingletonConfig(GetConfig()->ExecNode->JobProxy->JobProxyLogging->LogManagerTemplate);
        JobProxyConfigTemplate_->SetSingletonConfig(GetConfig()->ExecNode->JobProxy->JobProxyJaeger);

        JobProxyConfigTemplate_->OriginalClusterConnection = GetConfig()->ClusterConnection->Clone();

        JobProxyConfigTemplate_->ClusterConnection = GetConfig()->ClusterConnection->Clone();
        JobProxyConfigTemplate_->ClusterConnection->Static->OverrideMasterAddresses({localAddress});

        JobProxyConfigTemplate_->AuthenticationManager = GetConfig()->ExecNode->JobProxy->JobProxyAuthenticationManager;

        JobProxyConfigTemplate_->SupervisorConnection = New<NYT::NBus::TBusClientConfig>();
        JobProxyConfigTemplate_->SupervisorConnection->Address = localAddress;

        JobProxyConfigTemplate_->SupervisorRpcTimeout = GetConfig()->ExecNode->JobProxy->SupervisorRpcTimeout;

        JobProxyConfigTemplate_->HeartbeatPeriod = GetConfig()->ExecNode->JobProxy->JobProxyHeartbeatPeriod;

        JobProxyConfigTemplate_->SendHeartbeatBeforeAbort = GetConfig()->ExecNode->JobProxy->JobProxySendHeartbeatBeforeAbort;

        JobProxyConfigTemplate_->JobEnvironment = SlotManager_->GetJobEnvironmentConfig();

        JobProxyConfigTemplate_->StderrPath = GetConfig()->ExecNode->JobProxy->JobProxyLogging->JobProxyStderrPath;
        JobProxyConfigTemplate_->ExecutorStderrPath = GetConfig()->ExecNode->JobProxy->JobProxyLogging->ExecutorStderrPath;
        JobProxyConfigTemplate_->TestRootFS = GetConfig()->ExecNode->JobProxy->TestRootFS;
        JobProxyConfigTemplate_->AlwaysAbortOnMemoryReserveOverdraft = GetConfig()->ExecNode->JobProxy->AlwaysAbortOnMemoryReserveOverdraft;

        JobProxyConfigTemplate_->CoreWatcher = GetConfig()->ExecNode->JobProxy->CoreWatcher;

        JobProxyConfigTemplate_->TestPollJobShell = GetConfig()->ExecNode->JobProxy->TestPollJobShell;

        JobProxyConfigTemplate_->DoNotSetUserId = !SlotManager_->ShouldSetUserId();
        JobProxyConfigTemplate_->CheckUserJobMemoryLimit = GetConfig()->ExecNode->JobProxy->CheckUserJobMemoryLimit;
        JobProxyConfigTemplate_->ForwardAllEnvironmentVariables = GetConfig()->ExecNode->JobProxy->ForwardAllEnvironmentVariables;

        if (auto tvmService = NAuth::TNativeAuthenticationManager::Get()->GetTvmService()) {
            JobProxyConfigTemplate_->TvmBridgeConnection = New<NYT::NBus::TBusClientConfig>();
            JobProxyConfigTemplate_->TvmBridgeConnection->Address = localAddress;

            JobProxyConfigTemplate_->TvmBridge = New<NAuth::TTvmBridgeConfig>();
            JobProxyConfigTemplate_->TvmBridge->SelfTvmId = tvmService->GetSelfTvmId();
        }

        JobProxyConfigTemplate_->DnsOverRpcResolver = GetConfig()->ExecNode->JobProxy->JobProxyDnsOverRpcResolver;
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& oldConfig,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        YT_ASSERT_INVOKER_AFFINITY(GetControlInvoker());

        if (*oldConfig == *newConfig) {
            return;
        }

        ThrottlerManager_->Reconfigure(newConfig);

        // COMPAT(pogorelov)
        if (JobProxyLogManager_) {
            JobProxyLogManager_->OnDynamicConfigChanged(
                oldConfig->ExecNode->JobProxyLogManager,
                newConfig->ExecNode->JobProxyLogManager);
        }

        JobInputCache_->Reconfigure(newConfig->ExecNode->JobInputCache);
        JobController_->OnDynamicConfigChanged(
            oldConfig->ExecNode->JobController,
            newConfig->ExecNode->JobController);
        SlotManager_->OnDynamicConfigChanged(
            oldConfig->ExecNode->SlotManager,
            newConfig->ExecNode->SlotManager);
        ControllerAgentConnectorPool_->OnDynamicConfigChanged(
            oldConfig->ExecNode->ControllerAgentConnector,
            newConfig->ExecNode->ControllerAgentConnector);
        MasterConnector_->OnDynamicConfigChanged(
            oldConfig->ExecNode->MasterConnector,
            newConfig->ExecNode->MasterConnector);
        SchedulerConnector_->OnDynamicConfigChanged(
            oldConfig->ExecNode->SchedulerConnector,
            newConfig->ExecNode->SchedulerConnector);
        GpuManager_->OnDynamicConfigChanged(
            oldConfig->ExecNode->GpuManager,
            newConfig->ExecNode->GpuManager);
        JobReporter_->OnDynamicConfigChanged(
            oldConfig->ExecNode->JobReporter,
            newConfig->ExecNode->JobReporter);

        DynamicConfig_.Store(newConfig);

        if (NbdThreadPool_ && newConfig->ExecNode->Nbd) {
            NbdThreadPool_->SetThreadCount(newConfig->ExecNode->Nbd->Server->ThreadCount);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return New<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
