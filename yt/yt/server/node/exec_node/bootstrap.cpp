#include "bootstrap.h"

#include "chunk_cache.h"
#include "controller_agent_connector.h"
#include "exec_node_admin_service.h"
#include "gpu_manager.h"
#include "job.h"
#include "job_controller.h"
#include "job_prober_service.h"
#include "master_connector.h"
#include "orchid.h"
#include "private.h"
#include "scheduler_connector.h"
#include "slot_manager.h"
#include "supervisor_service.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/ytree_integration.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/misc/job_reporter.h>

#include <yt/yt/server/lib/nbd/config.h>
#include <yt/yt/server/lib/nbd/server.h>

#include <yt/yt/ytlib/auth/native_authentication_manager.h>
#include <yt/yt/ytlib/auth/tvm_bridge_service.h>

#include <yt/yt/ytlib/chunk_client/client_block_cache.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/library/dns_over_rpc/server/dns_over_rpc_service.h>

#include <yt/yt/library/containers/disk_manager/config.h>
#include <yt/yt/library/containers/disk_manager/disk_info_provider.h>
#include <yt/yt/library/containers/disk_manager/disk_manager_proxy.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

namespace NYT::NExecNode {

using namespace NApi;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NDataNode;
using namespace NJobAgent;
using namespace NJobProxy;
using namespace NNodeTrackerClient;
using namespace NProfiling;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;

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

        GetDynamicConfigManager()
            ->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));

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

        ControllerAgentConnectorPool_ = New<TControllerAgentConnectorPool>(this);

        BuildJobProxyConfigTemplate();

        ChunkCache_ = New<TChunkCache>(GetConfig()->DataNode, this);

        DynamicConfig_.Store(New<TClusterNodeDynamicConfig>());

        JobProxySolomonExporter_ = New<TSolomonExporter>(
            GetConfig()->ExecNode->JobProxySolomonExporter,
            New<TSolomonRegistry>());

        if (GetConfig()->EnableFairThrottler) {
            Throttlers_[EExecNodeThrottlerKind::JobIn] = ClusterNodeBootstrap_->GetInThrottler("job_in");
            Throttlers_[EExecNodeThrottlerKind::ArtifactCacheIn] = ClusterNodeBootstrap_->GetInThrottler("artifact_cache_in");
            Throttlers_[EExecNodeThrottlerKind::JobOut] = ClusterNodeBootstrap_->GetOutThrottler("job_out");
        } else {
            for (auto kind : TEnumTraits<EExecNodeThrottlerKind>::GetDomainValues()) {
                auto config = GetConfig()->DataNode->Throttlers[GetDataNodeThrottlerKind(kind)];
                config = ClusterNodeBootstrap_->PatchRelativeNetworkThrottlerConfig(config);

                RawThrottlers_[kind] = CreateNamedReconfigurableThroughputThrottler(
                    std::move(config),
                    ToString(kind),
                    ExecNodeLogger,
                    ExecNodeProfiler.WithPrefix("/throttlers"));

                auto throttler = IThroughputThrottlerPtr(RawThrottlers_[kind]);
                if (kind == EExecNodeThrottlerKind::ArtifactCacheIn || kind == EExecNodeThrottlerKind::JobIn) {
                    throttler = CreateCombinedThrottler({GetDefaultInThrottler(), throttler});
                } else if (kind == EExecNodeThrottlerKind::JobOut) {
                    throttler = CreateCombinedThrottler({GetDefaultOutThrottler(), throttler});
                }
                Throttlers_[kind] = throttler;
            }
        }

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

        DiskManagerProxy_ = CreateDiskManagerProxy(
            GetConfig()->DiskManagerProxy);
        DiskInfoProvider_ = New<NContainers::TDiskInfoProvider>(
            DiskManagerProxy_,
            GetConfig()->DiskInfoProvider);
        DiskChangeChecker_ = New<TDiskChangeChecker>(
            DiskInfoProvider_,
            GetControlInvoker(),
            ExecNodeLogger);

        // NB(psushin): initialize chunk cache first because slot manager (and root
        // volume manager inside it) can start using it to populate tmpfs layers cache.
        ChunkCache_->Initialize();
        SlotManager_->Initialize();
        JobController_->Initialize();
        MasterConnector_->Initialize();

        SubscribePopulateAlerts(BIND(&TDiskChangeChecker::PopulateAlerts, DiskChangeChecker_));
    }

    void Run() override
    {
        auto nbdConfig = DynamicConfig_.Acquire()->ExecNode->Nbd;
        if (nbdConfig && nbdConfig->Enabled) {
            NbdQueue_ = New<TActionQueue>("Nbd");
            NbdServer_ = CreateNbdServer(nbdConfig, NbdQueue_->GetInvoker());
        }

        SetNodeByYPath(
            GetOrchidRoot(),
            "/exec_node",
            CreateVirtualNode(GetOrchidService(this))
        );

        SetNodeByYPath(
            GetOrchidRoot(),
            "/disk_monitoring",
            CreateVirtualNode(DiskChangeChecker_->GetOrchidService()));

        SlotManager_->Start();
        JobController_->Start();

        JobProxySolomonExporter_->Register("/solomon/job_proxy", GetHttpServer());
        JobProxySolomonExporter_->Start();

        SchedulerConnector_->Start();

        ControllerAgentConnectorPool_->Start();
        DiskChangeChecker_->Start();
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

    virtual const TSchedulerConnectorPtr& GetSchedulerConnector() const override
    {
        return SchedulerConnector_;
    }

    const IThroughputThrottlerPtr& GetThrottler(EExecNodeThrottlerKind kind) const override
    {
        return Throttlers_[kind];
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

private:
    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;

    const TActionQueuePtr DnsOverRpcActionQueue_ = New<TActionQueue>("DnsOverRpc");

    TSlotManagerPtr SlotManager_;

    TGpuManagerPtr GpuManager_;

    TJobReporterPtr JobReporter_;

    TJobProxyInternalConfigPtr JobProxyConfigTemplate_;

    TChunkCachePtr ChunkCache_;

    IMasterConnectorPtr MasterConnector_;

    TSchedulerConnectorPtr SchedulerConnector_;

    IJobControllerPtr JobController_;

    TSolomonExporterPtr JobProxySolomonExporter_;

    TEnumIndexedArray<EExecNodeThrottlerKind, IReconfigurableThroughputThrottlerPtr> RawThrottlers_;
    TEnumIndexedArray<EExecNodeThrottlerKind, IThroughputThrottlerPtr> Throttlers_;

    TControllerAgentConnectorPoolPtr ControllerAgentConnectorPool_;

    //! TODO(arkady-e1ppa): Get rid of unneeded options in ExecNode config.
    //! then remove GetDynamicConfig method and make this non-atomic again.
    TAtomicIntrusivePtr<TClusterNodeDynamicConfig> DynamicConfig_;

    TActionQueuePtr NbdQueue_;
    NYT::NNbd::INbdServerPtr NbdServer_;

    NContainers::IDiskManagerProxyPtr DiskManagerProxy_;
    NContainers::TDiskInfoProviderPtr DiskInfoProvider_;
    TDiskChangeCheckerPtr DiskChangeChecker_;

    void BuildJobProxyConfigTemplate()
    {
        auto localAddress = NNet::BuildServiceAddress(NNet::GetLoopbackAddress(), GetConfig()->RpcPort);

        JobProxyConfigTemplate_ = New<NJobProxy::TJobProxyInternalConfig>();

        // Singletons.
        JobProxyConfigTemplate_->FiberStackPoolSizes = GetConfig()->FiberStackPoolSizes;
        JobProxyConfigTemplate_->AddressResolver = CloneYsonStruct(GetConfig()->AddressResolver);
        JobProxyConfigTemplate_->AddressResolver->LocalHostNameOverride = NNet::ReadLocalHostName();

        JobProxyConfigTemplate_->RpcDispatcher = GetConfig()->RpcDispatcher;
        JobProxyConfigTemplate_->TcpDispatcher = GetConfig()->TcpDispatcher;
        JobProxyConfigTemplate_->YPServiceDiscovery = GetConfig()->YPServiceDiscovery;
        JobProxyConfigTemplate_->ChunkClientDispatcher = GetConfig()->ChunkClientDispatcher;

        JobProxyConfigTemplate_->ClusterConnection = GetConfig()->ClusterConnection->Clone();
        JobProxyConfigTemplate_->OriginalClusterConnection = JobProxyConfigTemplate_->ClusterConnection->Clone();
        JobProxyConfigTemplate_->ClusterConnection->Static->OverrideMasterAddresses({localAddress});

        JobProxyConfigTemplate_->AuthenticationManager = GetConfig()->ExecNode->JobProxy->JobProxyAuthenticationManager;

        JobProxyConfigTemplate_->SupervisorConnection = New<NYT::NBus::TBusClientConfig>();
        JobProxyConfigTemplate_->SupervisorConnection->Address = localAddress;

        JobProxyConfigTemplate_->SupervisorRpcTimeout = GetConfig()->ExecNode->JobProxy->SupervisorRpcTimeout;

        JobProxyConfigTemplate_->HeartbeatPeriod = GetConfig()->ExecNode->JobProxy->JobProxyHeartbeatPeriod;

        JobProxyConfigTemplate_->SendHeartbeatBeforeAbort = GetConfig()->ExecNode->JobProxy->JobProxySendHeartbeatBeforeAbort;

        JobProxyConfigTemplate_->JobEnvironment = SlotManager_->GetJobEnvironmentConfig();

        JobProxyConfigTemplate_->Logging = GetConfig()->ExecNode->JobProxy->JobProxyLogging;
        JobProxyConfigTemplate_->Jaeger = GetConfig()->ExecNode->JobProxy->JobProxyJaeger;
        JobProxyConfigTemplate_->StderrPath = GetConfig()->ExecNode->JobProxy->JobProxyStderrPath;
        JobProxyConfigTemplate_->ExecutorStderrPath = GetConfig()->ExecNode->JobProxy->ExecutorStderrPath;
        JobProxyConfigTemplate_->TestRootFS = GetConfig()->ExecNode->JobProxy->TestRootFS;
        JobProxyConfigTemplate_->AlwaysAbortOnMemoryReserveOverdraft = GetConfig()->ExecNode->JobProxy->AlwaysAbortOnMemoryReserveOverdraft;

        JobProxyConfigTemplate_->CoreWatcher = GetConfig()->ExecNode->JobProxy->CoreWatcher;

        JobProxyConfigTemplate_->TestPollJobShell = GetConfig()->ExecNode->JobProxy->TestPollJobShell;

        JobProxyConfigTemplate_->DoNotSetUserId = !SlotManager_->ShouldSetUserId();
        JobProxyConfigTemplate_->CheckUserJobMemoryLimit = GetConfig()->ExecNode->JobProxy->CheckUserJobMemoryLimit;

        if (auto tvmService = NAuth::TNativeAuthenticationManager::Get()->GetTvmService()) {
            JobProxyConfigTemplate_->TvmBridgeConnection = New<NYT::NBus::TBusClientConfig>();
            JobProxyConfigTemplate_->TvmBridgeConnection->Address = localAddress;

            JobProxyConfigTemplate_->TvmBridge = New<NAuth::TTvmBridgeConfig>();
            JobProxyConfigTemplate_->TvmBridge->SelfTvmId = tvmService->GetSelfTvmId();
        }

        JobProxyConfigTemplate_->DnsOverRpcResolver = CloneYsonStruct(GetConfig()->ExecNode->JobProxy->JobProxyDnsOverRpcResolver);
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& oldConfig,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        VERIFY_INVOKER_AFFINITY(GetControlInvoker());

        if (!GetConfig()->EnableFairThrottler) {
            for (auto kind : TEnumTraits<EExecNodeThrottlerKind>::GetDomainValues()) {
                auto dataNodeThrottlerKind = GetDataNodeThrottlerKind(kind);
                auto config = newConfig->DataNode->Throttlers[dataNodeThrottlerKind]
                    ? newConfig->DataNode->Throttlers[dataNodeThrottlerKind]
                    : GetConfig()->DataNode->Throttlers[dataNodeThrottlerKind];
                config = ClusterNodeBootstrap_->PatchRelativeNetworkThrottlerConfig(config);
                RawThrottlers_[kind]->Reconfigure(std::move(config));
            }
        }

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

        DiskManagerProxy_->OnDynamicConfigChanged(newConfig->DiskManagerProxy);

        DynamicConfig_.Store(newConfig);
    }

    static EDataNodeThrottlerKind GetDataNodeThrottlerKind(EExecNodeThrottlerKind kind)
    {
        switch (kind) {
            case EExecNodeThrottlerKind::ArtifactCacheIn:
                return EDataNodeThrottlerKind::ArtifactCacheIn;
            case EExecNodeThrottlerKind::JobIn:
                return EDataNodeThrottlerKind::JobIn;
            case EExecNodeThrottlerKind::JobOut:
                return EDataNodeThrottlerKind::JobOut;
            default:
                YT_ABORT();
        }
    }

    NNbd::INbdServerPtr CreateNbdServer(TNbdConfigPtr nbdConfig, IInvokerPtr invoker)
    {
        NApi::NNative::TConnectionOptions connectionOptions;
        auto blockCacheConfig = New<NChunkClient::TBlockCacheConfig>();
        blockCacheConfig->CompressedData->Capacity = nbdConfig->BlockCacheCompressedDataCapacity;
        connectionOptions.BlockCache = CreateClientBlockCache(
            std::move(blockCacheConfig),
            NChunkClient::EBlockType::CompressedData);
        connectionOptions.ConnectionInvoker = invoker;
        auto connection = CreateConnection(TBootstrapBase::GetConnection()->GetCompoundConfig(), std::move(connectionOptions));
        connection->GetNodeDirectorySynchronizer()->Start();
        connection->GetClusterDirectorySynchronizer()->Start();

        return NNbd::CreateNbdServer(
            nbdConfig->Server,
            std::move(connection),
            NBus::TTcpDispatcher::Get()->GetXferPoller(),
            std::move(invoker));
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return std::make_unique<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
