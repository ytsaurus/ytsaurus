#include "bootstrap.h"
#include "config.h"
#include "batching_chunk_service.h"
#include "private.h"

#include <yt/server/data_node/blob_reader_cache.h>
#include <yt/server/data_node/block_cache.h>
#include <yt/server/data_node/chunk_block_manager.h>
#include <yt/server/data_node/chunk_cache.h>
#include <yt/server/data_node/chunk_registry.h>
#include <yt/server/data_node/chunk_store.h>
#include <yt/server/data_node/config.h>
#include <yt/server/data_node/data_node_service.h>
#include <yt/server/data_node/job.h>
#include <yt/server/data_node/journal_dispatcher.h>
#include <yt/server/data_node/location.h>
#include <yt/server/data_node/master_connector.h>
#include <yt/server/data_node/network_statistics.h>
#include <yt/server/data_node/peer_block_distributor.h>
#include <yt/server/data_node/peer_block_table.h>
#include <yt/server/data_node/peer_block_updater.h>
#include <yt/server/data_node/private.h>
#include <yt/server/data_node/session_manager.h>
#include <yt/server/data_node/ytree_integration.h>
#include <yt/server/data_node/chunk_meta_manager.h>
#include <yt/server/data_node/skynet_http_handler.h>

#include <yt/server/exec_agent/config.h>
#include <yt/server/exec_agent/job_environment.h>
#include <yt/server/exec_agent/job.h>
#include <yt/server/exec_agent/job_prober_service.h>
#include <yt/server/exec_agent/private.h>
#include <yt/server/exec_agent/scheduler_connector.h>
#include <yt/server/exec_agent/slot_manager.h>
#include <yt/server/exec_agent/supervisor_service.h>

#include <yt/server/job_agent/gpu_manager.h>
#include <yt/server/job_agent/job_controller.h>
#include <yt/server/job_agent/statistics_reporter.h>

#include <yt/server/misc/address_helpers.h>

#include <yt/server/object_server/master_cache_service.h>

#include <yt/server/query_agent/query_executor.h>
#include <yt/server/query_agent/query_service.h>

#include <yt/server/tablet_node/in_memory_manager.h>
#include <yt/server/tablet_node/in_memory_service.h>
#include <yt/server/tablet_node/partition_balancer.h>
#include <yt/server/tablet_node/security_manager.h>
#include <yt/server/tablet_node/slot_manager.h>
#include <yt/server/tablet_node/store_compactor.h>
#include <yt/server/tablet_node/store_flusher.h>
#include <yt/server/tablet_node/store_trimmer.h>
#include <yt/server/tablet_node/versioned_chunk_meta_manager.h>

#include <yt/server/transaction_server/timestamp_proxy_service.h>

#include <yt/server/admin_server/admin_service.h>

#include <yt/ytlib/program/build_attributes.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/client_block_cache.h>

#include <yt/ytlib/hydra/peer_channel.h>

#include <yt/ytlib/hive/cell_directory_synchronizer.h>

#include <yt/client/misc/workload.h>
#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/client/object_client/helpers.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/client/transaction_client/timestamp_provider.h>

#include <yt/ytlib/query_client/column_evaluator.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/ytlib/core_dump/core_dumper.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/core/bus/server.h>

#include <yt/core/bus/tcp/config.h>
#include <yt/core/bus/tcp/server.h>

#include <yt/core/http/server.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/net/address.h>

#include <yt/core/misc/collection_helpers.h>
#include <yt/core/misc/core_dumper.h>
#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/rpc/bus/channel.h>
#include <yt/core/rpc/bus/server.h>
#include <yt/core/rpc/caching_channel_factory.h>
#include <yt/core/rpc/channel.h>
#include <yt/core/rpc/redirector_service.h>
#include <yt/core/rpc/server.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/virtual.h>

#include <yt/core/alloc/alloc.h>

namespace NYT {
namespace NCellNode {

using namespace NAdmin;
using namespace NBus;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NChunkServer;
using namespace NElection;
using namespace NHydra;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;
using namespace NScheduler;
using namespace NJobAgent;
using namespace NExecAgent;
using namespace NJobProxy;
using namespace NDataNode;
using namespace NTabletNode;
using namespace NQueryAgent;
using namespace NApi;
using namespace NTransactionServer;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NObjectClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TCellNodeConfigPtr config, INodePtr configNode)
    : Config(std::move(config))
    , ConfigNode(std::move(configNode))
    , QueryThreadPool(BIND([this] () {
        return CreateFairShareThreadPool(Config->QueryAgent->ThreadPoolSize, "Query");
    }))
{
    WarnForUnrecognizedOptions(Logger, Config);
}

TBootstrap::~TBootstrap() = default;

void TBootstrap::Run()
{
    srand(time(nullptr));

    ControlQueue = New<TActionQueue>("Control");

    BIND(&TBootstrap::DoRun, this)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();

    Sleep(TDuration::Max());
}

void TBootstrap::DoRun()
{
    auto localRpcAddresses = NYT::GetLocalAddresses(Config->Addresses, Config->RpcPort);

    if (!Config->ClusterConnection->Networks) {
        Config->ClusterConnection->Networks = GetLocalNetworks();
    }

    LOG_INFO("Starting node (LocalAddresses: %v, PrimaryMasterAddresses: %v, NodeTags: %v)",
        GetValues(localRpcAddresses),
        Config->ClusterConnection->PrimaryMaster->Addresses,
        Config->Tags);

    MemoryUsageTracker = New<TNodeMemoryTracker>(
        Config->ResourceLimits->Memory,
        std::vector<std::pair<EMemoryCategory, i64>>{
            {EMemoryCategory::TabletStatic, Config->TabletNode->ResourceLimits->TabletStaticMemory},
            {EMemoryCategory::TabletDynamic, Config->TabletNode->ResourceLimits->TabletDynamicMemory}
        },
        Logger,
        TProfiler("/cell_node/memory_usage"));

    {
        auto result = MemoryUsageTracker->TryAcquire(EMemoryCategory::Footprint, Config->FootprintMemorySize);
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error reserving footprint memory");
    }

    FootprintUpdateExecutor = New<TPeriodicExecutor>(
        GetControlInvoker(),
        BIND(&TBootstrap::UpdateFootprintMemoryUsage, this),
        Config->FootprintUpdatePeriod);

    FootprintUpdateExecutor->Start();

    MasterConnection = NApi::NNative::CreateConnection(Config->ClusterConnection);

    if (Config->TabletNode->ResourceLimits->Slots > 0) {
        // Requesting latest timestamp enables periodic background time synchronization.
        // For tablet nodes, it is crucial because of non-atomic transactions that require
        // in-sync time for clients.
        GetLatestTimestamp();
    }

    MasterClient = MasterConnection->CreateNativeClient(TClientOptions(NSecurityClient::RootUserName));

    NodeDirectory = New<TNodeDirectory>();

    NodeDirectorySynchronizer = New<TNodeDirectorySynchronizer>(
        Config->NodeDirectorySynchronizer,
        MasterConnection,
        NodeDirectory);
    NodeDirectorySynchronizer->Start();

    LookupThreadPool = New<TThreadPool>(
        Config->QueryAgent->LookupThreadPoolSize,
        "Lookup");

    TableReplicatorThreadPool = New<TThreadPool>(
        Config->TabletNode->TabletManager->ReplicatorThreadPoolSize,
        "Replicator");

    TransactionTrackerQueue = New<TActionQueue>("TxTracker");

    BusServer = CreateTcpBusServer(Config->BusServer);

    RpcServer = NRpc::NBus::CreateBusServer(BusServer);

    Config->MonitoringServer->Port = Config->MonitoringPort;
    Config->MonitoringServer->BindRetryCount = Config->BusServer->BindRetryCount;
    Config->MonitoringServer->BindRetryBackoff = Config->BusServer->BindRetryBackoff;
    HttpServer = NHttp::CreateServer(
        Config->MonitoringServer);

    auto skynetHttpConfig = New<NHttp::TServerConfig>();
    skynetHttpConfig->Port = Config->SkynetHttpPort;
    skynetHttpConfig->BindRetryCount = Config->BusServer->BindRetryCount;
    skynetHttpConfig->BindRetryBackoff = Config->BusServer->BindRetryBackoff;
    SkynetHttpServer = NHttp::CreateServer(skynetHttpConfig);

    MonitoringManager_ = New<TMonitoringManager>();
    MonitoringManager_->Register(
        "/ref_counted",
        CreateRefCountedTrackerStatisticsProducer());

    auto createBatchingChunkService = [&] (const auto& config) {
        RpcServer->RegisterService(CreateBatchingChunkService(
            config->CellId,
            Config->BatchingChunkService,
            config,
            MasterConnection->GetChannelFactory()));
    };

    createBatchingChunkService(Config->ClusterConnection->PrimaryMaster);
    for (const auto& config : Config->ClusterConnection->SecondaryMasters) {
        createBatchingChunkService(config);
    }

    BlobReaderCache = New<TBlobReaderCache>(Config->DataNode);

    JournalDispatcher = New<TJournalDispatcher>(Config->DataNode);

    ChunkRegistry = New<TChunkRegistry>(this);

    ChunkMetaManager = New<TChunkMetaManager>(Config->DataNode, this);

    ChunkBlockManager = New<TChunkBlockManager>(Config->DataNode, this);

    NetworkStatistics = New<TNetworkStatistics>(Config->DataNode);

    BlockCache = CreateServerBlockCache(Config->DataNode, this);

    BlockMetaCache = New<TBlockMetaCache>(Config->DataNode->BlockMetaCache, TProfiler("/data_node/block_meta_cache"));

    PeerBlockDistributor = New<TPeerBlockDistributor>(Config->DataNode->PeerBlockDistributor, this);
    PeerBlockTable = New<TPeerBlockTable>(Config->DataNode->PeerBlockTable, this);
    PeerBlockUpdater = New<TPeerBlockUpdater>(Config->DataNode, this);

    SessionManager = New<TSessionManager>(Config->DataNode, this);

    MasterConnector = New<NDataNode::TMasterConnector>(
        Config->DataNode,
        localRpcAddresses,
        NYT::GetLocalAddresses(Config->Addresses, Config->SkynetHttpPort),
        NYT::GetLocalAddresses(Config->Addresses, Config->MonitoringPort),
        Config->Tags,
        this);
    MasterConnector->SubscribePopulateAlerts(BIND(&TBootstrap::PopulateAlerts, this));
    MasterConnector->SubscribeMasterConnected(BIND(&TBootstrap::OnMasterConnected, this));
    MasterConnector->SubscribeMasterDisconnected(BIND(&TBootstrap::OnMasterDisconnected, this));

    if (Config->CoreDumper) {
        CoreDumper = NCoreDump::CreateCoreDumper(Config->CoreDumper);
    }

    ChunkStore = New<NDataNode::TChunkStore>(Config->DataNode, this);

    ChunkCache = New<TChunkCache>(Config->DataNode, this);

    auto createThrottler = [] (const TThroughputThrottlerConfigPtr& config, const TString& name) {
        return CreateNamedReconfigurableThroughputThrottler(
            config,
            name,
            DataNodeLogger,
            DataNodeProfiler);
    };

    TotalInThrottler = createThrottler(Config->DataNode->TotalInThrottler, "TotalIn");
    TotalOutThrottler = createThrottler(Config->DataNode->TotalOutThrottler, "TotalOut");

    ReplicationInThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler,
        createThrottler(Config->DataNode->ReplicationInThrottler, "ReplicationIn")
    });
    ReplicationOutThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler,
        createThrottler(Config->DataNode->ReplicationOutThrottler, "ReplicationOut")
    });

    RepairInThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler,
        createThrottler(Config->DataNode->RepairInThrottler, "RepairIn")
    });
    RepairOutThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler,
        createThrottler(Config->DataNode->RepairOutThrottler, "RepairOut")
    });

    ArtifactCacheInThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler,
        createThrottler(Config->DataNode->ArtifactCacheInThrottler, "ArtifactCacheIn")
    });
    ArtifactCacheOutThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler,
        createThrottler(Config->DataNode->ArtifactCacheOutThrottler, "ArtifactCacheOut")
    });
    SkynetOutThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler,
        createThrottler(Config->DataNode->SkynetOutThrottler, "SkynetOut")
    });

    TabletCompactionAndPartitioningInThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler,
        createThrottler(Config->DataNode->TabletCompactionAndPartitioningInThrottler, "TabletCompactionAndPartitioningIn")
    });
    TabletCompactionAndPartitioningOutThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler,
        createThrottler(Config->DataNode->TabletCompactionAndPartitioningOutThrottler, "TabletCompactionAndPartitioningOut")
    });
    TabletLoggingInThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler,
        createThrottler(Config->DataNode->TabletLoggingInThrottler, "TabletLoggingIn")
    });
    TabletPreloadOutThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler,
        createThrottler(Config->DataNode->TabletPreloadOutThrottler, "TabletPreloadOut")
    });
    TabletSnapshotInThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler,
        createThrottler(Config->DataNode->TabletSnapshotInThrottler, "TabletSnapshotIn")
    });
    TabletStoreFlushInThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler,
        createThrottler(Config->DataNode->TabletStoreFlushInThrottler, "TabletStoreFlushIn")
    });
    TabletRecoveryOutThrottler = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler,
        createThrottler(Config->DataNode->TabletRecoveryOutThrottler, "TabletRecoveryOut")
    });

    ReadRpsOutThrottler = createThrottler(Config->DataNode->ReadRpsOutThrottler, "ReadRpsOut");

    RpcServer->RegisterService(CreateDataNodeService(Config->DataNode, this));

    RpcServer->RegisterService(CreateInMemoryService(Config->TabletNode->InMemoryManager, this));

    auto localAddress = GetDefaultAddress(localRpcAddresses);

    JobProxyConfigTemplate = New<NJobProxy::TJobProxyConfig>();

    // Singletons.
    JobProxyConfigTemplate->FiberStackPoolSizes = Config->FiberStackPoolSizes;
    JobProxyConfigTemplate->AddressResolver = Config->AddressResolver;
    JobProxyConfigTemplate->RpcDispatcher = Config->RpcDispatcher;
    JobProxyConfigTemplate->ChunkClientDispatcher = Config->ChunkClientDispatcher;
    JobProxyConfigTemplate->JobThrottler = Config->JobThrottler;

    JobProxyConfigTemplate->ClusterConnection = CloneYsonSerializable(Config->ClusterConnection);

    auto patchMasterConnectionConfig = [&] (const NNative::TMasterConnectionConfigPtr& config) {
        config->Addresses = {localAddress};
        if (config->RetryTimeout && *config->RetryTimeout > config->RpcTimeout) {
            config->RpcTimeout = *config->RetryTimeout;
        }
        config->RetryTimeout = Null;
        config->RetryAttempts = 1;
    };

    patchMasterConnectionConfig(JobProxyConfigTemplate->ClusterConnection->PrimaryMaster);
    for (const auto& config : JobProxyConfigTemplate->ClusterConnection->SecondaryMasters) {
        patchMasterConnectionConfig(config);
    }

    JobProxyConfigTemplate->SupervisorConnection = New<NYT::NBus::TTcpBusClientConfig>();

    JobProxyConfigTemplate->SupervisorConnection->Address = localAddress;

    JobProxyConfigTemplate->SupervisorRpcTimeout = Config->ExecAgent->SupervisorRpcTimeout;

    JobProxyConfigTemplate->HeartbeatPeriod = Config->ExecAgent->JobProxyHeartbeatPeriod;

    JobProxyConfigTemplate->JobEnvironment = Config->ExecAgent->SlotManager->JobEnvironment;

    JobProxyConfigTemplate->JobCpuMonitor = Config->ExecAgent->JobCpuMonitor;

    JobProxyConfigTemplate->Logging = Config->ExecAgent->JobProxyLogging;
    JobProxyConfigTemplate->Tracing = Config->ExecAgent->JobProxyTracing;
    JobProxyConfigTemplate->TestRootFS = Config->ExecAgent->TestRootFS;

    JobProxyConfigTemplate->CoreForwarderTimeout = Config->ExecAgent->CoreForwarderTimeout;

    ExecSlotManager = New<NExecAgent::TSlotManager>(Config->ExecAgent->SlotManager, this);
    GpuManager = New<TGpuManager>();

    JobController = New<TJobController>(Config->ExecAgent->JobController, this);

    auto createExecJob = BIND([this] (
            const NJobAgent::TJobId& jobId,
            const NJobAgent::TOperationId& operationId,
            const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
            NJobTrackerClient::NProto::TJobSpec&& jobSpec) ->
            NJobAgent::IJobPtr
        {
            return NExecAgent::CreateUserJob(
                jobId,
                operationId,
                resourceLimits,
                std::move(jobSpec),
                this);
        });
    JobController->RegisterFactory(NJobAgent::EJobType::Map,               createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::PartitionMap,      createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::SortedMerge,       createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::OrderedMerge,      createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::UnorderedMerge,    createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::Partition,         createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::SimpleSort,        createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::IntermediateSort,  createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::FinalSort,         createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::SortedReduce,      createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::PartitionReduce,   createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::ReduceCombiner,    createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::RemoteCopy,        createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::OrderedMap,        createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::JoinReduce,        createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::Vanilla,           createExecJob);

    auto createChunkJob = BIND([this] (
            const NJobAgent::TJobId& jobId,
            const NJobAgent::TOperationId& /*operationId*/,
            const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
            NJobTrackerClient::NProto::TJobSpec&& jobSpec) ->
            NJobAgent::IJobPtr
        {
            return NDataNode::CreateChunkJob(
                jobId,
                std::move(jobSpec),
                resourceLimits,
                Config->DataNode,
                this);
        });
    JobController->RegisterFactory(NJobAgent::EJobType::RemoveChunk,     createChunkJob);
    JobController->RegisterFactory(NJobAgent::EJobType::ReplicateChunk,  createChunkJob);
    JobController->RegisterFactory(NJobAgent::EJobType::RepairChunk,     createChunkJob);
    JobController->RegisterFactory(NJobAgent::EJobType::SealChunk,       createChunkJob);

    StatisticsReporter = New<TStatisticsReporter>(
        Config->ExecAgent->StatisticsReporter,
        this);

    RpcServer->RegisterService(CreateJobProberService(this));

    RpcServer->RegisterService(New<TSupervisorService>(this));

    SchedulerConnector = New<TSchedulerConnector>(Config->ExecAgent->SchedulerConnector, this);

    ColumnEvaluatorCache = New<NQueryClient::TColumnEvaluatorCache>(
        New<NQueryClient::TColumnEvaluatorCacheConfig>());

    TabletSlotManager = New<NTabletNode::TSlotManager>(Config->TabletNode, this);
    MasterConnector->SubscribePopulateAlerts(BIND(&NTabletNode::TSlotManager::PopulateAlerts, TabletSlotManager));

    SecurityManager = New<TSecurityManager>(Config->TabletNode->SecurityManager, this);

    InMemoryManager = CreateInMemoryManager(Config->TabletNode->InMemoryManager, this);

    VersionedChunkMetaManager = New<TVersionedChunkMetaManager>(Config->TabletNode, this);

    QueryExecutor = CreateQuerySubexecutor(Config->QueryAgent, this);

    RpcServer->RegisterService(CreateQueryService(Config->QueryAgent, this));

    RpcServer->RegisterService(CreateTimestampProxyService(
        MasterConnection->GetTimestampProvider()));

    auto initMasterCacheSerivce = [&] (const auto& masterConfig) {
        return CreateMasterCacheService(
            Config->MasterCacheService,
            CreateDefaultTimeoutChannel(
                CreatePeerChannel(
                    masterConfig,
                    MasterConnection->GetChannelFactory(),
                    EPeerKind::Follower),
                masterConfig->RpcTimeout),
            masterConfig->CellId);
    };

    MasterCacheServices.push_back(initMasterCacheSerivce(
        Config->ClusterConnection->PrimaryMaster));

    for (const auto& masterConfig : Config->ClusterConnection->SecondaryMasters) {
        MasterCacheServices.push_back(initMasterCacheSerivce(masterConfig));
    }

    OrchidRoot = GetEphemeralNodeFactory(true)->CreateMap();

    SetNodeByYPath(
        OrchidRoot,
        "/monitoring",
        CreateVirtualNode(MonitoringManager_->GetService()));
    SetNodeByYPath(
        OrchidRoot,
        "/profiling",
        CreateVirtualNode(TProfileManager::Get()->GetService()));
    SetNodeByYPath(
        OrchidRoot,
        "/config",
        ConfigNode);
    SetNodeByYPath(
        OrchidRoot,
        "/stored_chunks",
        CreateVirtualNode(CreateStoredChunkMapService(ChunkStore)
            ->Via(GetControlInvoker())));
    SetNodeByYPath(
        OrchidRoot,
        "/cached_chunks",
        CreateVirtualNode(CreateCachedChunkMapService(ChunkCache)
            ->Via(GetControlInvoker())));
    SetNodeByYPath(
        OrchidRoot,
        "/tablet_cells",
        CreateVirtualNode(TabletSlotManager->GetOrchidService()));
    SetNodeByYPath(
        OrchidRoot,
        "/job_controller",
        CreateVirtualNode(JobController->GetOrchidService()
            ->Via(GetControlInvoker())));
    SetBuildAttributes(OrchidRoot, "node");

    HttpServer->AddHandler(
        "/orchid/",
        NMonitoring::GetOrchidYPathHttpHandler(OrchidRoot));

    SkynetHttpServer->AddHandler(
        "/read_skynet_part",
        MakeSkynetHttpHandler(this));

    RpcServer->RegisterService(CreateOrchidService(
        OrchidRoot,
        GetControlInvoker()));

    RpcServer->RegisterService(CreateAdminService(GetControlInvoker(), CoreDumper));

    LOG_INFO("Listening for HTTP requests on port %v", Config->MonitoringPort);

    LOG_INFO("Listening for RPC requests on port %v", Config->RpcPort);
    RpcServer->Configure(Config->RpcServer);

    // Do not start subsystems until everything is initialized.
    TabletSlotManager->Initialize();
    ChunkStore->Initialize();
    ChunkCache->Initialize();
    ExecSlotManager->Initialize();
    JobController->Initialize();
    MonitoringManager_->Start();
    PeerBlockUpdater->Start();
    PeerBlockDistributor->Start();
    MasterConnector->Start();
    SchedulerConnector->Start();
    StartStoreFlusher(Config->TabletNode, this);
    StartStoreCompactor(Config->TabletNode, this);
    StartStoreTrimmer(Config->TabletNode, this);
    StartPartitionBalancer(Config->TabletNode, this);

    RpcServer->Start();
    HttpServer->Start();
    SkynetHttpServer->Start();
}

const TCellNodeConfigPtr& TBootstrap::GetConfig() const
{
    return Config;
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlQueue->GetInvoker();
}

IInvokerPtr TBootstrap::GetQueryPoolInvoker(const TFairShareThreadPoolTag& tag) const
{
    return QueryThreadPool->GetInvoker(tag);
}

const IInvokerPtr& TBootstrap::GetLookupPoolInvoker() const
{
    return LookupThreadPool->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetTableReplicatorPoolInvoker() const
{
    return TableReplicatorThreadPool->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetTransactionTrackerInvoker() const
{
    return TransactionTrackerQueue->GetInvoker();
}

const NNative::IClientPtr& TBootstrap::GetMasterClient() const
{
    return MasterClient;
}

const NNative::IConnectionPtr& TBootstrap::GetMasterConnection() const
{
    return MasterConnection;
}

const IServerPtr& TBootstrap::GetRpcServer() const
{
    return RpcServer;
}

const IMapNodePtr& TBootstrap::GetOrchidRoot() const
{
    return OrchidRoot;
}

const TJobControllerPtr& TBootstrap::GetJobController() const
{
    return JobController;
}

const TStatisticsReporterPtr& TBootstrap::GetStatisticsReporter() const
{
    return StatisticsReporter;
}

const NTabletNode::TSlotManagerPtr& TBootstrap::GetTabletSlotManager() const
{
    return TabletSlotManager;
}

const TSecurityManagerPtr& TBootstrap::GetSecurityManager() const
{
    return SecurityManager;
}

const IInMemoryManagerPtr& TBootstrap::GetInMemoryManager() const
{
    return InMemoryManager;
}

const TVersionedChunkMetaManagerPtr& TBootstrap::GetVersionedChunkMetaManager() const
{
    return VersionedChunkMetaManager;
}

const NExecAgent::TSlotManagerPtr& TBootstrap::GetExecSlotManager() const
{
    return ExecSlotManager;
}

const NJobAgent::TGpuManagerPtr& TBootstrap::GetGpuManager() const
{
    return GpuManager;
}

const TChunkStorePtr& TBootstrap::GetChunkStore() const
{
    return ChunkStore;
}

const TChunkCachePtr& TBootstrap::GetChunkCache() const
{
    return ChunkCache;
}

TNodeMemoryTracker* TBootstrap::GetMemoryUsageTracker() const
{
    return MemoryUsageTracker.Get();
}

const TChunkRegistryPtr& TBootstrap::GetChunkRegistry() const
{
    return ChunkRegistry;
}

const TSessionManagerPtr& TBootstrap::GetSessionManager() const
{
    return SessionManager;
}

const TChunkBlockManagerPtr& TBootstrap::GetChunkBlockManager() const
{
    return ChunkBlockManager;
}

const TNetworkStatisticsPtr& TBootstrap::GetNetworkStatistics() const
{
    return NetworkStatistics;
}

const TChunkMetaManagerPtr& TBootstrap::GetChunkMetaManager() const
{
    return ChunkMetaManager;
}

const IBlockCachePtr& TBootstrap::GetBlockCache() const
{
    return BlockCache;
}

const TBlockMetaCachePtr& TBootstrap::GetBlockMetaCache() const
{
    return BlockMetaCache;
}

const TPeerBlockDistributorPtr& TBootstrap::GetPeerBlockDistributor() const
{
    return PeerBlockDistributor;
}

const TPeerBlockTablePtr& TBootstrap::GetPeerBlockTable() const
{
    return PeerBlockTable;
}

const TPeerBlockUpdaterPtr& TBootstrap::GetPeerBlockUpdater() const
{
    return PeerBlockUpdater;
}

const TBlobReaderCachePtr& TBootstrap::GetBlobReaderCache() const
{
    return BlobReaderCache;
}

const TJournalDispatcherPtr& TBootstrap::GetJournalDispatcher() const
{
    return JournalDispatcher;
}

const TMasterConnectorPtr& TBootstrap::GetMasterConnector() const
{
    return MasterConnector;
}

const TNodeDirectoryPtr& TBootstrap::GetNodeDirectory() const
{
    return NodeDirectory;
}

const NQueryClient::ISubexecutorPtr& TBootstrap::GetQueryExecutor() const
{
    return QueryExecutor;
}

const TCellId& TBootstrap::GetCellId() const
{
    return Config->ClusterConnection->PrimaryMaster->CellId;
}

TCellId TBootstrap::GetCellId(TCellTag cellTag) const
{
    return cellTag == PrimaryMasterCellTag
        ? GetCellId()
        : ReplaceCellTagInId(GetCellId(), cellTag);
}

const NQueryClient::TColumnEvaluatorCachePtr& TBootstrap::GetColumnEvaluatorCache() const
{
    return ColumnEvaluatorCache;
}

const IThroughputThrottlerPtr& TBootstrap::GetReplicationInThrottler() const
{
    return ReplicationInThrottler;
}

const IThroughputThrottlerPtr& TBootstrap::GetReplicationOutThrottler() const
{
    return ReplicationOutThrottler;
}

const IThroughputThrottlerPtr& TBootstrap::GetRepairInThrottler() const
{
    return RepairInThrottler;
}

const IThroughputThrottlerPtr& TBootstrap::GetRepairOutThrottler() const
{
    return RepairOutThrottler;
}

const IThroughputThrottlerPtr& TBootstrap::GetArtifactCacheInThrottler() const
{
    return ArtifactCacheInThrottler;
}

const IThroughputThrottlerPtr& TBootstrap::GetArtifactCacheOutThrottler() const
{
    return ArtifactCacheOutThrottler;
}

const IThroughputThrottlerPtr& TBootstrap::GetSkynetOutThrottler() const
{
    return SkynetOutThrottler;
}

const IThroughputThrottlerPtr& TBootstrap::GetInThrottler(const TWorkloadDescriptor& descriptor) const
{
    switch (descriptor.Category) {
        case EWorkloadCategory::SystemRepair:
            return RepairInThrottler;

        case EWorkloadCategory::SystemReplication:
            return ReplicationInThrottler;

        case EWorkloadCategory::SystemArtifactCacheDownload:
            return ArtifactCacheInThrottler;

        case EWorkloadCategory::SystemTabletCompaction:
        case EWorkloadCategory::SystemTabletPartitioning:
            return TabletCompactionAndPartitioningInThrottler;

        case EWorkloadCategory::SystemTabletLogging:
            return TabletLoggingInThrottler;

        case EWorkloadCategory::SystemTabletSnapshot:
            return TabletSnapshotInThrottler;

        case EWorkloadCategory::SystemTabletStoreFlush:
            return TabletStoreFlushInThrottler;

        default:
            return TotalInThrottler;
    }
}

const IThroughputThrottlerPtr& TBootstrap::GetOutThrottler(const TWorkloadDescriptor& descriptor) const
{
    switch (descriptor.Category) {
        case EWorkloadCategory::SystemRepair:
            return RepairOutThrottler;

        case EWorkloadCategory::SystemReplication:
            return ReplicationOutThrottler;

        case EWorkloadCategory::SystemArtifactCacheDownload:
            return ArtifactCacheOutThrottler;

        case EWorkloadCategory::SystemTabletCompaction:
        case EWorkloadCategory::SystemTabletPartitioning:
            return TabletCompactionAndPartitioningOutThrottler;

        case EWorkloadCategory::SystemTabletPreload:
            return TabletPreloadOutThrottler;

        case EWorkloadCategory::SystemTabletRecovery:
            return TabletRecoveryOutThrottler;

        default:
            return TotalOutThrottler;
    }
}

const IThroughputThrottlerPtr& TBootstrap::GetReadRpsOutThrottler() const
{
    return ReadRpsOutThrottler;
}

TNetworkPreferenceList TBootstrap::GetLocalNetworks()
{
    return Config->Addresses.empty()
        ? DefaultNetworkPreferences
        : GetIths<0>(Config->Addresses);
}

TNullable<TString> TBootstrap::GetDefaultNetworkName()
{
    return Config->BusServer->DefaultNetwork;
}

TJobProxyConfigPtr TBootstrap::BuildJobProxyConfig() const
{
    auto proxyConfig = CloneYsonSerializable(JobProxyConfigTemplate);
    auto localDescriptor = GetMasterConnector()->GetLocalDescriptor();
    proxyConfig->DataCenter = localDescriptor.GetDataCenter();
    proxyConfig->Rack = localDescriptor.GetRack();
    proxyConfig->Addresses = localDescriptor.Addresses();
    return proxyConfig;
}

TTimestamp TBootstrap::GetLatestTimestamp() const
{
    return MasterConnection
        ->GetTimestampProvider()
        ->GetLatestTimestamp();
}

void TBootstrap::PopulateAlerts(std::vector<TError>* alerts)
{
    // NB: Don't expect IsXXXExceeded helpers to be atomic.
    auto totalUsed = MemoryUsageTracker->GetTotalUsed();
    auto totalLimit = MemoryUsageTracker->GetTotalLimit();
    if (totalUsed > totalLimit) {
        alerts->push_back(TError("Total memory limit exceeded")
            << TErrorAttribute("used", totalUsed)
            << TErrorAttribute("limit", totalLimit));
    }

    for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
        auto used = MemoryUsageTracker->GetUsed(category);
        auto limit = MemoryUsageTracker->GetLimit(category);
        if (used > limit) {
            alerts->push_back(TError("Memory limit exceeded for category %Qlv",
                category)
                << TErrorAttribute("used", used)
                << TErrorAttribute("limit", limit));
        }
    }
}

void TBootstrap::OnMasterConnected()
{
    for (const auto& masterCacheService : MasterCacheServices) {
        RpcServer->RegisterService(masterCacheService);
    }
}

void TBootstrap::OnMasterDisconnected()
{
    for (const auto& masterCacheService : MasterCacheServices) {
        RpcServer->UnregisterService(masterCacheService);
    }
}

void TBootstrap::UpdateFootprintMemoryUsage()
{
    auto bytesCommitted = NYTAlloc::GetTotalCounters()[NYTAlloc::ETotalCounter::BytesCommitted];
    auto newFootprint = Config->FootprintMemorySize + bytesCommitted;
    for (auto memoryCategory : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
        if (memoryCategory == EMemoryCategory::UserJobs || memoryCategory == EMemoryCategory::Footprint) {
            continue;
        }
        newFootprint -= GetMemoryUsageTracker()->GetUsed(memoryCategory);
    }

    auto oldFootprint = GetMemoryUsageTracker()->GetUsed(EMemoryCategory::Footprint);

    LOG_INFO("Memory footprint updated (BytesCommitted: %v, OldFootprint: %v, NewFootprint: %v)",
        bytesCommitted,
        oldFootprint,
        newFootprint);

    if (newFootprint > oldFootprint) {
        GetMemoryUsageTracker()->Acquire(
            EMemoryCategory::Footprint,
            newFootprint - oldFootprint);
    } else {
        GetMemoryUsageTracker()->Release(
            EMemoryCategory::Footprint,
            oldFootprint - newFootprint);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
