#include "bootstrap.h"

#include "artifact_cache.h"
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

#include <yt/yt/server/lib/signature/components.h>
#include <yt/yt/server/lib/signature/config.h>

#include <yt/yt/ytlib/auth/native_authentication_manager.h>
#include <yt/yt/ytlib/auth/tvm_bridge_service.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/validator.h>

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
using namespace NSignature;
using namespace NThreading;
using namespace NObjectClient;
using namespace NCellMasterClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ExecNodeLogger;

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
        SubscribeSecondaryMasterCellListChanged(
            BIND_NO_PROPAGATE(&TBootstrap::OnSecondaryMasterCellListChanged, MakeStrong(this)));

        SlotManager_ = New<TSlotManager>(this);

        GpuManager_ = New<TGpuManager>(this);
        GpuManager_->Initialize();

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

        BuildJobProxyConfigTemplate(/*secondaryMasterConfigsOverrides*/ std::nullopt);

        ArtifactCache_ = New<TArtifactCache>(GetConfig()->DataNode, this);

        DynamicConfig_.Store(New<TClusterNodeDynamicConfig>());

        // Host and InstanceTags are passed to job proxy and processed separately.
        auto jobProxySolomonExporterConfig = CloneYsonStruct(GetConfig()->ExecNode->JobProxySolomonExporter);
        jobProxySolomonExporterConfig->Host = std::nullopt;
        jobProxySolomonExporterConfig->InstanceTags = {};

        JobProxySolomonExporter_ = New<TSolomonExporter>(
            jobProxySolomonExporterConfig,
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

        auto ownerId = TOwnerId(NNet::BuildServiceAddress(GetLocalHostName(), GetConfig()->RpcPort));
        SignatureComponents_ = New<TSignatureComponents>(
            GetConfig()->ExecNode->SignatureComponents,
            std::move(ownerId),
            GetConnection(),
            GetControlInvoker());

        // NB(psushin): initialize chunk cache first because slot manager (and root
        // volume manager inside it) can start using it to populate tmpfs layers cache.
        ArtifactCache_->Initialize();
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
            auto connection = ClusterNodeBootstrap_->GetClusterNodeBootstrap()->GetConnection();
            auto clientOptions = NYT::NApi::NNative::TClientOptions::FromUser(NSecurityClient::RootUserName);
            auto client = connection->CreateNativeClient(clientOptions);

            auto blockCacheConfig = New<TBlockCacheConfig>();
            blockCacheConfig->CompressedData->Capacity = nbdConfig->BlockCacheCompressedDataCapacity;

            auto layerBlockCache = CreateClientBlockCache(
                blockCacheConfig,
                EBlockType::CompressedData,
                GetNullMemoryUsageTracker(),
                ExecNodeProfiler().WithPrefix("/layer_block_cache"));

            LayerReaderHost_ = New<TChunkReaderHost>(
                client,
                /*localDescriptor*/ NNodeTrackerClient::TNodeDescriptor{},
                std::move(layerBlockCache),
                connection->GetChunkMetaCache(),
                MakeUniformPerCategoryThrottlerProvider(GetDefaultInThrottler()),
                GetReadRpsOutThrottler(),
                /*mediumThrottler*/ nullptr,
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
                MakeUniformPerCategoryThrottlerProvider(GetDefaultInThrottler()),
                GetReadRpsOutThrottler(),
                /*mediumThrottler*/ nullptr,
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

        WaitFor(SignatureComponents_->StartRotation())
            .ThrowOnError();

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

    TJobProxyInternalConfigPtr GetJobProxyConfigTemplate() const override
    {
        return JobProxyConfigTemplate_.Acquire();
    }

    const TArtifactCachePtr& GetArtifactCache() const override
    {
        return ArtifactCache_;
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
        TClusterName clusterName) const override
    {
        return ThrottlerManager_->GetOrCreateThrottler(kind, trafficType, std::move(clusterName));
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

    ISignatureGeneratorPtr GetSignatureGenerator() const override
    {
        return SignatureComponents_->GetSignatureGenerator();
    }

    ISignatureValidatorPtr GetSignatureValidator() const override
    {
        return SignatureComponents_->GetSignatureValidator();
    }

private:
    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;

    const TActionQueuePtr DnsOverRpcActionQueue_ = New<TActionQueue>("DnsOverRpc");

    IJobInputCachePtr JobInputCache_;

    TSlotManagerPtr SlotManager_;

    TGpuManagerPtr GpuManager_;

    TJobReporterPtr JobReporter_;

    TAtomicIntrusivePtr<TJobProxyInternalConfig> JobProxyConfigTemplate_;

    TArtifactCachePtr ArtifactCache_;

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

    TSignatureComponentsPtr SignatureComponents_;

    void BuildJobProxyConfigTemplate(const std::optional<TSecondaryMasterConnectionConfigs>& optionalNewSecondaryMasterConfigs)
    {
        auto localAddress = NNet::BuildServiceAddress(NNet::GetLoopbackAddress(), GetConfig()->RpcPort);

        auto oldJobProxyConfigTemplate = GetJobProxyConfigTemplate();
        auto newJobProxyConfigTemplate = New<NJobProxy::TJobProxyInternalConfig>();

        newJobProxyConfigTemplate->MergeAllSingletonConfigsFrom(*TSingletonManager::GetConfig());
        {
            auto config = newJobProxyConfigTemplate->GetSingletonConfig<NNet::TAddressResolverConfig>();
            config->LocalHostNameOverride = NNet::GetLocalHostName();
        }

        newJobProxyConfigTemplate->SetSingletonConfig(GetConfig()->ExecNode->JobProxy->JobProxyLogging->LogManagerTemplate);
        newJobProxyConfigTemplate->SetSingletonConfig(GetConfig()->ExecNode->JobProxy->JobProxyJaeger);

        if (const auto& clusterConnection = GetConfig()->ExecNode->JobProxy->ClusterConnection) {
            newJobProxyConfigTemplate->OriginalClusterConnection = clusterConnection->Clone();
        } else {
            newJobProxyConfigTemplate->OriginalClusterConnection = GetConfig()->ClusterConnection->Clone();
        }

        // We could probably replace addresses for known cells here as well, but
        // changing addresses of a known cell is cursed anyway, so I'm not
        // doing that now.
        if (optionalNewSecondaryMasterConfigs) {
            auto newSecondaryMasterConfigs = *optionalNewSecondaryMasterConfigs;
            for (const auto& secondaryMasterConfig : newJobProxyConfigTemplate->OriginalClusterConnection->Static->SecondaryMasters) {
                auto cellTag = CellTagFromId(secondaryMasterConfig->CellId);
                if (newSecondaryMasterConfigs.erase(cellTag)) {
                    YT_LOG_ALERT("Config contains a cell that was reported as new (CellTag: %v)", cellTag);
                }
            }

            newJobProxyConfigTemplate->OriginalClusterConnection->Static->SecondaryMasters = oldJobProxyConfigTemplate->OriginalClusterConnection->Static->SecondaryMasters;
            for (const auto& [cellTag, config] : newSecondaryMasterConfigs) {
                newJobProxyConfigTemplate->OriginalClusterConnection->Static->SecondaryMasters.push_back(config);
            }
        }
        newJobProxyConfigTemplate->ClusterConnection = newJobProxyConfigTemplate->OriginalClusterConnection->Clone();
        newJobProxyConfigTemplate->ClusterConnection->Static->OverrideMasterAddresses({localAddress});

        newJobProxyConfigTemplate->AuthenticationManager = GetConfig()->ExecNode->JobProxy->JobProxyAuthenticationManager;

        if (const auto& supervisorConnection = GetConfig()->ExecNode->JobProxy->SupervisorConnection) {
            newJobProxyConfigTemplate->SupervisorConnection = CloneYsonStruct(supervisorConnection);
        } else {
            newJobProxyConfigTemplate->SupervisorConnection = New<NYT::NBus::TBusClientConfig>();
            newJobProxyConfigTemplate->SupervisorConnection->Address = localAddress;
        }

        newJobProxyConfigTemplate->SupervisorRpcTimeout = GetConfig()->ExecNode->JobProxy->SupervisorRpcTimeout;

        newJobProxyConfigTemplate->HeartbeatPeriod = GetConfig()->ExecNode->JobProxy->JobProxyHeartbeatPeriod;

        newJobProxyConfigTemplate->SendHeartbeatBeforeAbort = GetConfig()->ExecNode->JobProxy->JobProxySendHeartbeatBeforeAbort;

        newJobProxyConfigTemplate->JobEnvironment = SlotManager_->GetJobEnvironmentConfig();

        newJobProxyConfigTemplate->StderrPath = GetConfig()->ExecNode->JobProxy->JobProxyLogging->JobProxyStderrPath;
        newJobProxyConfigTemplate->ExecutorStderrPath = GetConfig()->ExecNode->JobProxy->JobProxyLogging->ExecutorStderrPath;
        newJobProxyConfigTemplate->TestRootFS = GetConfig()->ExecNode->JobProxy->TestRootFS;
        newJobProxyConfigTemplate->AlwaysAbortOnMemoryReserveOverdraft = GetConfig()->ExecNode->JobProxy->AlwaysAbortOnMemoryReserveOverdraft;

        newJobProxyConfigTemplate->CoreWatcher = GetConfig()->ExecNode->JobProxy->CoreWatcher;

        newJobProxyConfigTemplate->TestPollJobShell = GetConfig()->ExecNode->JobProxy->TestPollJobShell;

        newJobProxyConfigTemplate->DoNotSetUserId = !SlotManager_->ShouldSetUserId();
        newJobProxyConfigTemplate->CheckUserJobMemoryLimit = GetConfig()->ExecNode->JobProxy->CheckUserJobMemoryLimit;
        newJobProxyConfigTemplate->ForwardAllEnvironmentVariables = GetConfig()->ExecNode->JobProxy->ForwardAllEnvironmentVariables;
        newJobProxyConfigTemplate->EnvironmentVariables = GetConfig()->ExecNode->JobProxy->EnvironmentVariables;

        if (auto tvmService = NAuth::TNativeAuthenticationManager::Get()->GetTvmService()) {
            newJobProxyConfigTemplate->TvmBridgeConnection = New<NYT::NBus::TBusClientConfig>();
            newJobProxyConfigTemplate->TvmBridgeConnection->Address = localAddress;

            // Duplicate the TBusConfig part from SupervisorConnection to TvmBridgeConnection,
            // because it may have ssl parameters.
            if (const NBus::TBusConfigPtr& busConfig = GetConfig()->ExecNode->JobProxy->SupervisorConnection) {
                newJobProxyConfigTemplate->TvmBridgeConnection = UpdateYsonStruct(
                    newJobProxyConfigTemplate->TvmBridgeConnection,
                    NYson::ConvertToYsonString(busConfig));
            }

            newJobProxyConfigTemplate->TvmBridge = New<NAuth::TTvmBridgeConfig>();
            newJobProxyConfigTemplate->TvmBridge->SelfTvmId = tvmService->GetSelfTvmId();
        }

        newJobProxyConfigTemplate->DnsOverRpcResolver = GetConfig()->ExecNode->JobProxy->JobProxyDnsOverRpcResolver;

        newJobProxyConfigTemplate->SolomonExporter->Host = GetConfig()->ExecNode->JobProxySolomonExporter->Host;
        newJobProxyConfigTemplate->SolomonExporter->InstanceTags = GetConfig()->ExecNode->JobProxySolomonExporter->InstanceTags;

        JobProxyConfigTemplate_.Store(std::move(newJobProxyConfigTemplate));
    }

    void OnSecondaryMasterCellListChanged(
        const TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs)
    {
        YT_ASSERT_THREAD_AFFINITY(ControlThread);

        BuildJobProxyConfigTemplate(newSecondaryMasterConfigs);
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
        if (newConfig->ExecNode->SignatureComponents) {
            YT_UNUSED_FUTURE(SignatureComponents_->Reconfigure(newConfig->ExecNode->SignatureComponents));
        }

        DynamicConfig_.Store(newConfig);

        if (NbdThreadPool_ && newConfig->ExecNode->Nbd) {
            NbdThreadPool_->SetThreadCount(newConfig->ExecNode->Nbd->Server->ThreadCount);
        }
    }

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return New<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
