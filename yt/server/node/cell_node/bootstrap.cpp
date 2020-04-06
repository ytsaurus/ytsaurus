#include "bootstrap.h"
#include "config.h"
#include "batching_chunk_service.h"
#include "dynamic_config_manager.h"
#include "resource_manager.h"
#include "private.h"

#include <yt/server/lib/exec_agent/config.h>

#include <yt/server/node/data_node/blob_reader_cache.h>
#include <yt/server/node/data_node/block_cache.h>
#include <yt/server/node/data_node/chunk_block_manager.h>
#include <yt/server/node/data_node/chunk_cache.h>
#include <yt/server/node/data_node/chunk_registry.h>
#include <yt/server/node/data_node/chunk_store.h>
#include <yt/server/node/data_node/config.h>
#include <yt/server/node/data_node/data_node_service.h>
#include <yt/server/node/data_node/job.h>
#include <yt/server/node/data_node/journal_dispatcher.h>
#include <yt/server/node/data_node/location.h>
#include <yt/server/node/data_node/master_connector.h>
#include <yt/server/node/data_node/network_statistics.h>
#include <yt/server/node/data_node/peer_block_distributor.h>
#include <yt/server/node/data_node/peer_block_table.h>
#include <yt/server/node/data_node/peer_block_updater.h>
#include <yt/server/node/data_node/private.h>
#include <yt/server/node/data_node/session_manager.h>
#include <yt/server/node/data_node/table_schema_cache.h>
#include <yt/server/node/data_node/ytree_integration.h>
#include <yt/server/node/data_node/chunk_meta_manager.h>
#include <yt/server/node/data_node/skynet_http_handler.h>

#include <yt/server/node/exec_agent/job_environment.h>
#include <yt/server/node/exec_agent/job.h>
#include <yt/server/node/exec_agent/job_prober_service.h>
#include <yt/server/node/exec_agent/private.h>
#include <yt/server/node/exec_agent/scheduler_connector.h>
#include <yt/server/node/exec_agent/slot_manager.h>
#include <yt/server/node/exec_agent/supervisor_service.h>

#include <yt/server/node/job_agent/gpu_manager.h>
#include <yt/server/node/job_agent/job_controller.h>
#include <yt/server/lib/job_agent/job_reporter.h>

#include <yt/server/lib/misc/address_helpers.h>

#include <yt/server/lib/object_server/master_cache_service.h>

#include <yt/server/node/query_agent/query_executor.h>
#include <yt/server/node/query_agent/query_service.h>

#include <yt/server/node/tablet_node/in_memory_manager.h>
#include <yt/server/node/tablet_node/in_memory_service.h>
#include <yt/server/node/tablet_node/partition_balancer.h>
#include <yt/server/node/tablet_node/security_manager.h>
#include <yt/server/node/tablet_node/slot_manager.h>
#include <yt/server/node/tablet_node/store_compactor.h>
#include <yt/server/node/tablet_node/store_flusher.h>
#include <yt/server/node/tablet_node/store_trimmer.h>
#include <yt/server/node/tablet_node/tablet_cell_service.h>
#include <yt/server/node/tablet_node/versioned_chunk_meta_manager.h>

#include <yt/server/lib/transaction_server/timestamp_proxy_service.h>

#include <yt/server/lib/admin/admin_service.h>

#include <yt/server/lib/containers/instance_limits_tracker.h>
#include <yt/server/lib/containers/porto_executor.h>

#include <yt/server/lib/core_dump/core_dumper.h>

#include <yt/server/lib/hydra/snapshot.h>
#include <yt/server/lib/hydra/file_snapshot_store.h>

#include <yt/server/lib/object_server/object_service_cache.h>

#include <yt/ytlib/program/build_attributes.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/client_block_cache.h>

#include <yt/ytlib/hydra/peer_channel.h>

#include <yt/ytlib/hive/cell_directory_synchronizer.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/ytlib/query_client/column_evaluator.h>

#include <yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/client/misc/workload.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/transaction_client/config.h>
#include <yt/client/transaction_client/timestamp_provider.h>
#include <yt/client/transaction_client/remote_timestamp_provider.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/bus/server.h>

#include <yt/core/bus/tcp/config.h>
#include <yt/core/bus/tcp/server.h>

#include <yt/core/http/server.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/fair_share_thread_pool.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/net/address.h>

#include <yt/core/misc/collection_helpers.h>
#include <yt/core/misc/core_dumper.h>
#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/core/ytalloc/statistics_producer.h>
#include <yt/core/ytalloc/bindings.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/rpc/bus/channel.h>
#include <yt/core/rpc/bus/server.h>
#include <yt/core/rpc/caching_channel_factory.h>
#include <yt/core/rpc/channel.h>
#include <yt/core/rpc/redirector_service.h>
#include <yt/core/rpc/server.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/virtual.h>

#include <library/ytalloc/api/ytalloc.h>

namespace NYT::NCellNode {

using namespace NAdmin;
using namespace NBus;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NContainers;
using namespace NNodeTrackerClient;
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
using namespace NObjectServer;
using namespace NTableClient;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TCellNodeConfigPtr config, INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
    , QueryThreadPool_(BIND([this] () {
        return CreateTwoLevelFairShareThreadPool(Config_->QueryAgent->ThreadPoolSize, "Query");
    }))
{ }

TBootstrap::~TBootstrap() = default;

void TBootstrap::Initialize()
{
    srand(time(nullptr));

    ControlQueue_ = New<TActionQueue>("Control");

    BIND(&TBootstrap::DoInitialize, this)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}
void TBootstrap::Run()
{
    BIND(&TBootstrap::DoRun, this)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();

    Sleep(TDuration::Max());
}

void TBootstrap::ValidateSnapshot(const TString& fileName)
{
    BIND(&TBootstrap::DoValidateSnapshot, this, fileName)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

bool TBootstrap::IsReadOnly() const
{
    return !DynamicConfigManager_->IsDynamicConfigLoaded();
}

void TBootstrap::DoInitialize()
{
    auto localRpcAddresses = NYT::GetLocalAddresses(Config_->Addresses, Config_->RpcPort);

    if (!Config_->ClusterConnection->Networks) {
        Config_->ClusterConnection->Networks = GetLocalNetworks();
    }

    YT_LOG_INFO("Initializing node (LocalAddresses: %v, PrimaryMasterAddresses: %v, NodeTags: %v)",
        GetValues(localRpcAddresses),
        Config_->ClusterConnection->PrimaryMaster->Addresses,
        Config_->Tags);

    NodeResourceManager_ = New<TNodeResourceManager>(GetControlInvoker(), this, Config_->ResourceLimitsUpdatePeriod);
    if (Config_->InstanceLimitsUpdatePeriod) {
        auto portoExecutorConfig = ConvertTo<TPortoJobEnvironmentConfigPtr>(Config_->ExecAgent->SlotManager->JobEnvironment)->PortoExecutor;
        auto portoExecutor = CreatePortoExecutor(
            portoExecutorConfig,
            "limits_tracker");
        InstanceLimitsTracker_ = CreateSelfPortoInstanceLimitsTracker(
            portoExecutor,
            GetControlInvoker(),
            *Config_->InstanceLimitsUpdatePeriod);
        InstanceLimitsTracker_->SubscribeLimitsUpdated(BIND(&TNodeResourceManager::OnInstanceLimitsUpdated, NodeResourceManager_)
            .Via(GetControlInvoker()));
    }

    MemoryUsageTracker_ = New<TNodeMemoryTracker>(
        Config_->ResourceLimits->TotalMemory,
        std::vector<std::pair<EMemoryCategory, i64>>{},
        Logger,
        TProfiler("/cell_node/memory_usage"));

    MasterConnection_ = NApi::NNative::CreateConnection(Config_->ClusterConnection);
    MasterClient_ = MasterConnection_->CreateNativeClient(TClientOptions(NSecurityClient::RootUserName));

    MasterCacheQueue_ = New<TActionQueue>("MasterCache");
    JobThrottlerQueue_ = New<TActionQueue>("JobThrottler");
    TabletLookupThreadPool_ = New<TThreadPool>(
        Config_->QueryAgent->LookupThreadPoolSize,
        "TabletLookup",
        true,
        true,
        EInvokerQueueType::SingleLockFreeQueue);
    TableReplicatorThreadPool_ = New<TThreadPool>(
        Config_->TabletNode->TabletManager->ReplicatorThreadPoolSize,
        "Replicator");
    TransactionTrackerQueue_ = New<TActionQueue>("TxTracker");
    StorageHeavyThreadPool_ = New<TThreadPool>(
        Config_->DataNode->StorageHeavyThreadCount,
        "StorageHeavy");
    StorageHeavyInvoker_ = CreatePrioritizedInvoker(StorageHeavyThreadPool_->GetInvoker());
    StorageLightThreadPool_ = New<TThreadPool>(
        Config_->DataNode->StorageLightThreadCount,
        "StorageLight");
    StorageLookupThreadPool_ = CreateFairShareThreadPool(
        Config_->DataNode->StorageLookupThreadCount,
        "StorageLookup");

    BusServer_ = CreateTcpBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

    auto createBatchingChunkService = [&] (const auto& config) {
        RpcServer_->RegisterService(CreateBatchingChunkService(
            config->CellId,
            Config_->BatchingChunkService,
            config,
            MasterConnection_->GetChannelFactory()));
    };

    createBatchingChunkService(Config_->ClusterConnection->PrimaryMaster);
    for (const auto& config : Config_->ClusterConnection->SecondaryMasters) {
        createBatchingChunkService(config);
    }

    BlobReaderCache_ = New<TBlobReaderCache>(Config_->DataNode, this);

    TableSchemaCache_ = New<TTableSchemaCache>(Config_->DataNode->TableSchemaCache);

    JournalDispatcher_ = New<TJournalDispatcher>(Config_->DataNode);

    ChunkRegistry_ = New<TChunkRegistry>(this);

    ChunkMetaManager_ = New<TChunkMetaManager>(Config_->DataNode, this);

    ChunkBlockManager_ = New<TChunkBlockManager>(Config_->DataNode, this);

    NetworkStatistics_ = std::make_unique<TNetworkStatistics>(Config_->DataNode);

    BlockCache_ = CreateServerBlockCache(Config_->DataNode, this);

    BlockMetaCache_ = New<TBlockMetaCache>(Config_->DataNode->BlockMetaCache, TProfiler("/data_node/block_meta_cache"));

    PeerBlockDistributor_ = New<TPeerBlockDistributor>(Config_->DataNode->PeerBlockDistributor, this);
    PeerBlockTable_ = New<TPeerBlockTable>(Config_->DataNode->PeerBlockTable, this);
    PeerBlockUpdater_ = New<TPeerBlockUpdater>(Config_->DataNode, this);

    SessionManager_ = New<TSessionManager>(Config_->DataNode, this);

    MasterConnector_ = New<NDataNode::TMasterConnector>(
        Config_->DataNode,
        localRpcAddresses,
        NYT::GetLocalAddresses(Config_->Addresses, Config_->SkynetHttpPort),
        NYT::GetLocalAddresses(Config_->Addresses, Config_->MonitoringPort),
        Config_->Tags,
        this);
    MasterConnector_->SubscribePopulateAlerts(BIND(&TBootstrap::PopulateAlerts, this));
    MasterConnector_->SubscribeMasterConnected(BIND(&TBootstrap::OnMasterConnected, this));
    MasterConnector_->SubscribeMasterDisconnected(BIND(&TBootstrap::OnMasterDisconnected, this));

    DynamicConfigManager_ = New<TDynamicConfigManager>(Config_->DynamicConfigManager, this);
    DynamicConfigManager_->SubscribeConfigUpdated(BIND(&TBootstrap::OnDynamicConfigUpdated, this));
    DynamicConfigManager_->Start();

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    ChunkStore_ = New<NDataNode::TChunkStore>(Config_->DataNode, this);

    ChunkCache_ = New<TChunkCache>(Config_->DataNode, this);

    auto netThrottlerProfiler = DataNodeProfiler.AppendPath("/net_throttler");
    auto createThrottler = [&] (const TThroughputThrottlerConfigPtr& config, const TString& name) {
        return CreateNamedReconfigurableThroughputThrottler(
            config,
            name,
            DataNodeLogger,
            netThrottlerProfiler);
    };

    TotalInThrottler_ = createThrottler(Config_->DataNode->TotalInThrottler, "TotalIn");
    TotalOutThrottler_ = createThrottler(Config_->DataNode->TotalOutThrottler, "TotalOut");

    ReplicationInThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler_,
        createThrottler(Config_->DataNode->ReplicationInThrottler, "ReplicationIn")
    });
    ReplicationOutThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler_,
        createThrottler(Config_->DataNode->ReplicationOutThrottler, "ReplicationOut")
    });

    RepairInThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler_,
        createThrottler(Config_->DataNode->RepairInThrottler, "RepairIn")
    });
    RepairOutThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler_,
        createThrottler(Config_->DataNode->RepairOutThrottler, "RepairOut")
    });

    ArtifactCacheInThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler_,
        createThrottler(Config_->DataNode->ArtifactCacheInThrottler, "ArtifactCacheIn")
    });
    ArtifactCacheOutThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler_,
        createThrottler(Config_->DataNode->ArtifactCacheOutThrottler, "ArtifactCacheOut")
    });
    SkynetOutThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler_,
        createThrottler(Config_->DataNode->SkynetOutThrottler, "SkynetOut")
    });

    DataNodeTabletCompactionAndPartitioningInThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler_,
        createThrottler(Config_->DataNode->TabletCompactionAndPartitioningInThrottler, "DataNodeTabletCompactionAndPartitioningIn")
    });
    DataNodeTabletCompactionAndPartitioningOutThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler_,
        createThrottler(Config_->DataNode->TabletCompactionAndPartitioningOutThrottler, "TabletCompactionAndPartitioningOut")
    });
    DataNodeTabletLoggingInThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler_,
        createThrottler(Config_->DataNode->TabletLoggingInThrottler, "DataNodeTabletLoggingIn")
    });
    DataNodeTabletPreloadOutThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler_,
        createThrottler(Config_->DataNode->TabletPreloadOutThrottler, "DataNodeTabletPreloadOut")
    });
    DataNodeTabletSnapshotInThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler_,
        createThrottler(Config_->DataNode->TabletSnapshotInThrottler, "DataNodeTabletSnapshotIn")
    });
    DataNodeTabletStoreFlushInThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler_,
        createThrottler(Config_->DataNode->TabletStoreFlushInThrottler, "DataNodeTabletStoreFlushIn")
    });
    DataNodeTabletRecoveryOutThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler_,
        createThrottler(Config_->DataNode->TabletRecoveryOutThrottler, "DataNodeTabletRecoveryOut")
    });
    DataNodeTabletReplicationOutThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler_,
        createThrottler(Config_->DataNode->TabletReplicationOutThrottler, "DataNodeTabletReplicationOut")
    });

    TabletNodeCompactionAndPartitioningInThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler_,
        createThrottler(Config_->TabletNode->StoreCompactionAndPartitioningInThrottler, "TabletNodeCompactionAndPartitioningIn")
    });
    TabletNodeCompactionAndPartitioningOutThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler_,
        createThrottler(Config_->TabletNode->StoreCompactionAndPartitioningOutThrottler, "TabletNodeCompactionAndPartitioningOut")
    });
    TabletNodeStoreFlushOutThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler_,
        createThrottler(Config_->TabletNode->StoreFlushOutThrottler, "TabletNodeStoreFlushOut")
    });
    TabletNodePreloadInThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler_,
        createThrottler(Config_->TabletNode->InMemoryManager->PreloadThrottler, "TabletNodePreloadIn")
    });
    TabletNodeTabletReplicationInThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalInThrottler_,
        createThrottler(Config_->TabletNode->ReplicationInThrottler, "TabletNodeReplicationIn")
    });
    TabletNodeTabletReplicationOutThrottler_ = CreateCombinedThrottler(std::vector<IThroughputThrottlerPtr>{
        TotalOutThrottler_,
        createThrottler(Config_->TabletNode->ReplicationOutThrottler, "TabletNodeReplicationOut")
    });

    ReadRpsOutThrottler_ = createThrottler(Config_->DataNode->ReadRpsOutThrottler, "ReadRpsOut");

    RpcServer_->RegisterService(CreateDataNodeService(Config_->DataNode, this));

    RpcServer_->RegisterService(CreateInMemoryService(Config_->TabletNode->InMemoryManager, this));

    auto localAddress = GetDefaultAddress(localRpcAddresses);

    if (GetEnvironmentType() == EJobEnvironmentType::Porto) {
        auto* resolver = TAddressResolver::Get();
        ResolvedNodeAddresses_.reserve(Config_->Addresses.size());
        for (const auto& [addressName, address] : Config_->Addresses) {
            auto resolvedAddress = resolver->Resolve(address).Get().ValueOrThrow();
            YT_VERIFY(resolvedAddress.IsIP6());
            ResolvedNodeAddresses_.emplace_back(resolvedAddress.ToIP6Address());
        }
    }

    JobProxyConfigTemplate_ = New<NJobProxy::TJobProxyConfig>();

    // Singletons.
    JobProxyConfigTemplate_->FiberStackPoolSizes = Config_->FiberStackPoolSizes;
    JobProxyConfigTemplate_->AddressResolver = Config_->AddressResolver;
    JobProxyConfigTemplate_->RpcDispatcher = Config_->RpcDispatcher;
    JobProxyConfigTemplate_->ChunkClientDispatcher = Config_->ChunkClientDispatcher;
    JobProxyConfigTemplate_->JobThrottler = Config_->JobThrottler;

    JobProxyConfigTemplate_->ClusterConnection = CloneYsonSerializable(Config_->ClusterConnection);
    JobProxyConfigTemplate_->ClusterConnection->MasterCellDirectorySynchronizer->RetryPeriod = std::nullopt;

    auto patchMasterConnectionConfig = [&] (const NNative::TMasterConnectionConfigPtr& config) {
        config->Addresses = {localAddress};
        if (config->RetryTimeout && *config->RetryTimeout > config->RpcTimeout) {
            config->RpcTimeout = *config->RetryTimeout;
        }
        config->RetryTimeout = std::nullopt;
        config->RetryAttempts = 1;
    };

    patchMasterConnectionConfig(JobProxyConfigTemplate_->ClusterConnection->PrimaryMaster);
    for (const auto& config : JobProxyConfigTemplate_->ClusterConnection->SecondaryMasters) {
        patchMasterConnectionConfig(config);
    }
    if (JobProxyConfigTemplate_->ClusterConnection->MasterCache) {
        patchMasterConnectionConfig(JobProxyConfigTemplate_->ClusterConnection->MasterCache);
        JobProxyConfigTemplate_->ClusterConnection->MasterCache->EnableMasterCacheDiscovery = false;
    }

    JobProxyConfigTemplate_->SupervisorConnection = New<NYT::NBus::TTcpBusClientConfig>();

    JobProxyConfigTemplate_->SupervisorConnection->Address = localAddress;

    JobProxyConfigTemplate_->SupervisorRpcTimeout = Config_->ExecAgent->SupervisorRpcTimeout;

    JobProxyConfigTemplate_->HeartbeatPeriod = Config_->ExecAgent->JobProxyHeartbeatPeriod;

    JobProxyConfigTemplate_->JobEnvironment = Config_->ExecAgent->SlotManager->JobEnvironment;

    JobProxyConfigTemplate_->Logging = Config_->ExecAgent->JobProxyLogging;
    JobProxyConfigTemplate_->Tracing = Config_->ExecAgent->JobProxyTracing;
    JobProxyConfigTemplate_->TestRootFS = Config_->ExecAgent->TestRootFS;

    JobProxyConfigTemplate_->CoreWatcher = Config_->ExecAgent->CoreWatcher;

    ExecSlotManager_ = New<NExecAgent::TSlotManager>(Config_->ExecAgent->SlotManager, this);
    GpuManager_ = New<TGpuManager>(this, Config_->ExecAgent->JobController->GpuManager);

    JobController_ = New<TJobController>(Config_->ExecAgent->JobController, this);

    auto createExecJob = BIND([this] (
            NJobAgent::TJobId jobId,
            NJobAgent::TOperationId operationId,
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
    JobController_->RegisterJobFactory(NJobAgent::EJobType::Map, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::PartitionMap, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::SortedMerge, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::OrderedMerge, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::UnorderedMerge, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::Partition, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::SimpleSort, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::IntermediateSort, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::FinalSort, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::SortedReduce, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::PartitionReduce, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::ReduceCombiner, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::RemoteCopy, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::OrderedMap, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::JoinReduce, createExecJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::Vanilla, createExecJob);

    auto createChunkJob = BIND([this] (
            NJobAgent::TJobId jobId,
            NJobAgent::TOperationId /*operationId*/,
            const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
            NJobTrackerClient::NProto::TJobSpec&& jobSpec) ->
            NJobAgent::IJobPtr
        {
            return NDataNode::CreateChunkJob(
                jobId,
                std::move(jobSpec),
                resourceLimits,
                Config_->DataNode,
                this);
        });
    JobController_->RegisterJobFactory(NJobAgent::EJobType::RemoveChunk, createChunkJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::ReplicateChunk, createChunkJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::RepairChunk, createChunkJob);
    JobController_->RegisterJobFactory(NJobAgent::EJobType::SealChunk, createChunkJob);

    JobReporter_ = New<TJobReporter>(
        Config_->ExecAgent->JobReporter,
        this->GetMasterConnection(),
        this->GetMasterConnector()->GetLocalDescriptor().GetDefaultAddress());

    RpcServer_->RegisterService(CreateJobProberService(this));

    RpcServer_->RegisterService(CreateSupervisorService(this));

    SchedulerConnector_ = New<TSchedulerConnector>(Config_->ExecAgent->SchedulerConnector, this);

    ColumnEvaluatorCache_ = New<NQueryClient::TColumnEvaluatorCache>(
        New<NQueryClient::TColumnEvaluatorCacheConfig>());

    TabletSlotManager_ = New<NTabletNode::TSlotManager>(Config_->TabletNode, this);
    MasterConnector_->SubscribePopulateAlerts(BIND(&NTabletNode::TSlotManager::PopulateAlerts, TabletSlotManager_));

    SecurityManager_ = New<TSecurityManager>(Config_->TabletNode->SecurityManager, this);

    InMemoryManager_ = CreateInMemoryManager(Config_->TabletNode->InMemoryManager, this);

    VersionedChunkMetaManager_ = New<TVersionedChunkMetaManager>(Config_->TabletNode, this);

    QueryExecutor_ = CreateQuerySubexecutor(Config_->QueryAgent, this);

    RpcServer_->RegisterService(CreateQueryService(Config_->QueryAgent, this));

    auto timestampProviderConfig = CreateBatchingRemoteTimestampProviderConfig(Config_->ClusterConnection->PrimaryMaster);
    auto timestampProvider = CreateBatchingRemoteTimestampProvider(
        timestampProviderConfig,
        CreateTimestampProviderChannel(timestampProviderConfig, MasterConnection_->GetChannelFactory()));
    RpcServer_->RegisterService(CreateTimestampProxyService(timestampProvider));

    auto cache = New<TObjectServiceCache>(
        Config_->MasterCacheService,
        Logger,
        TProfiler("/cell_node/master_cache"));

    {
        auto result = GetMemoryUsageTracker()->TryAcquire(EMemoryCategory::MasterCache, Config_->MasterCacheService->Capacity);
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error reserving memory for master cache");
    }

    auto initMasterCacheService = [&] (const auto& masterConfig) {
        return CreateMasterCacheService(
            Config_->MasterCacheService,
            MasterCacheQueue_->GetInvoker(),
            CreateDefaultTimeoutChannel(
                CreatePeerChannel(
                    masterConfig,
                    MasterConnection_->GetChannelFactory(),
                    EPeerKind::Follower),
                masterConfig->RpcTimeout),
            cache,
            masterConfig->CellId);
    };

    MasterCacheServices_.push_back(initMasterCacheService(
        Config_->ClusterConnection->PrimaryMaster));

    for (const auto& masterConfig : Config_->ClusterConnection->SecondaryMasters) {
        MasterCacheServices_.push_back(initMasterCacheService(masterConfig));
    }

    RpcServer_->RegisterService(CreateTabletCellService(this));

    RpcServer_->RegisterService(CreateAdminService(GetControlInvoker(), CoreDumper_));

    RpcServer_->Configure(Config_->RpcServer);

    TabletSlotManager_->Initialize();
    ChunkStore_->Initialize();
    ChunkCache_->Initialize();
    ExecSlotManager_->Initialize();
    JobController_->Initialize();
}

void TBootstrap::DoRun()
{
    auto localRpcAddresses = NYT::GetLocalAddresses(Config_->Addresses, Config_->RpcPort);

    YT_LOG_INFO("Starting node (LocalAddresses: %v, PrimaryMasterAddresses: %v, NodeTags: %v)",
        GetValues(localRpcAddresses),
        Config_->ClusterConnection->PrimaryMaster->Addresses,
        Config_->Tags);

    NodeResourceManager_->Start();
    if (InstanceLimitsTracker_) {
        InstanceLimitsTracker_->Start();
    }

    // Force start node directory synchronizer.
    MasterConnection_->GetNodeDirectorySynchronizer()->Start();

    if (Config_->TabletNode->ResourceLimits->Slots > 0) {
        // Requesting latest timestamp enables periodic background time synchronization.
        // For tablet nodes, it is crucial because of non-atomic transactions that require
        // in-sync time for clients.
        GetLatestTimestamp();
    }

    Config_->MonitoringServer->Port = Config_->MonitoringPort;
    Config_->MonitoringServer->BindRetryCount = Config_->BusServer->BindRetryCount;
    Config_->MonitoringServer->BindRetryBackoff = Config_->BusServer->BindRetryBackoff;
    Config_->MonitoringServer->ServerName = "monitoring";
    HttpServer_ = NHttp::CreateServer(
        Config_->MonitoringServer);

    auto skynetHttpConfig = New<NHttp::TServerConfig>();
    skynetHttpConfig->Port = Config_->SkynetHttpPort;
    skynetHttpConfig->BindRetryCount = Config_->BusServer->BindRetryCount;
    skynetHttpConfig->BindRetryBackoff = Config_->BusServer->BindRetryBackoff;
    skynetHttpConfig->ServerName = "skynet";
    SkynetHttpServer_ = NHttp::CreateServer(skynetHttpConfig);

    NMonitoring::Initialize(HttpServer_, &MonitoringManager_, &OrchidRoot_);

    auto storeCompactor = CreateStoreCompactor(Config_->TabletNode, this);

    SetNodeByYPath(
        OrchidRoot_,
        "/config",
        ConfigNode_);
    SetNodeByYPath(
        OrchidRoot_,
        "/stored_chunks",
        CreateVirtualNode(CreateStoredChunkMapService(ChunkStore_)
            ->Via(GetControlInvoker())));
    SetNodeByYPath(
        OrchidRoot_,
        "/cached_chunks",
        CreateVirtualNode(CreateCachedChunkMapService(ChunkCache_)
            ->Via(GetControlInvoker())));
    SetNodeByYPath(
        OrchidRoot_,
        "/tablet_cells",
        CreateVirtualNode(TabletSlotManager_->GetOrchidService()));
    SetNodeByYPath(
        OrchidRoot_,
        "/job_controller",
        CreateVirtualNode(JobController_->GetOrchidService()
            ->Via(GetControlInvoker())));
    SetNodeByYPath(
        OrchidRoot_,
        "/cluster_connection",
        CreateVirtualNode(MasterConnection_->GetOrchidService()));
    SetNodeByYPath(
        OrchidRoot_,
        "/store_compactor",
        CreateVirtualNode(GetOrchidService(storeCompactor)));
    SetNodeByYPath(
        OrchidRoot_,
        "/dynamic_config_manager",
        CreateVirtualNode(DynamicConfigManager_->GetOrchidService()
            ->Via(GetControlInvoker())));

    SetBuildAttributes(OrchidRoot_, "node");

    SkynetHttpServer_->AddHandler(
        "/read_skynet_part",
        MakeSkynetHttpHandler(this));

    RpcServer_->RegisterService(CreateOrchidService(
        OrchidRoot_,
        GetControlInvoker()));

    YT_LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);

    YT_LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);

    // Do not start subsystems until everything is initialized.
    PeerBlockUpdater_->Start();
    PeerBlockDistributor_->Start();
    MasterConnector_->Start();
    SchedulerConnector_->Start();

    StartStoreFlusher(Config_->TabletNode, this);
    StartStoreCompactor(storeCompactor);
    StartStoreTrimmer(Config_->TabletNode, this);
    StartPartitionBalancer(Config_->TabletNode, this);

    RpcServer_->Start();
    HttpServer_->Start();
    SkynetHttpServer_->Start();

    DoValidateConfig();
}

void TBootstrap::DoValidateConfig()
{
    auto unrecognized = Config_->GetUnrecognizedRecursively();
    if (unrecognized && unrecognized->GetChildCount() > 0) {
        if (Config_->EnableUnrecognizedOptionsAlert) {
            MasterConnector_->RegisterAlert(TError(EErrorCode::UnrecognizedConfigOption, "Node config contains unrecognized options")
                << TErrorAttribute("unrecognized", unrecognized));
        }
        if (Config_->AbortOnUnrecognizedOptions) {
            YT_LOG_ERROR("Node config contains unrecognized options, aborting (Unrecognized: %v)",
                ConvertToYsonString(unrecognized, NYson::EYsonFormat::Text));
            YT_ABORT();
        } else {
            YT_LOG_WARNING("Node config contains unrecognized options (Unrecognized: %v)",
                ConvertToYsonString(unrecognized, NYson::EYsonFormat::Text));
        }
    }
}

void TBootstrap::DoValidateSnapshot(const TString& fileName)
{
    auto reader = CreateFileSnapshotReader(
        fileName,
        InvalidSegmentId,
        false /*isRaw*/,
        std::nullopt /*offset*/,
        true /*skipHeader*/);

    WaitFor(reader->Open())
        .ThrowOnError();

    GetTabletSlotManager()->ValidateCellSnapshot(reader);
}

const TCellNodeConfigPtr& TBootstrap::GetConfig() const
{
    return Config_;
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

IInvokerPtr TBootstrap::GetQueryPoolInvoker(
    const TString& poolName,
    double weight,
    const TFairShareThreadPoolTag& tag) const
{
    return QueryThreadPool_->GetInvoker(poolName, weight, tag);
}

const IInvokerPtr& TBootstrap::GetTabletLookupPoolInvoker() const
{
    return TabletLookupThreadPool_->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetTableReplicatorPoolInvoker() const
{
    return TableReplicatorThreadPool_->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetTransactionTrackerInvoker() const
{
    return TransactionTrackerQueue_->GetInvoker();
}

const IPrioritizedInvokerPtr& TBootstrap::GetStorageHeavyInvoker() const
{
    return StorageHeavyInvoker_;
}

const IInvokerPtr& TBootstrap::GetStorageLightInvoker() const
{
    return StorageLightThreadPool_->GetInvoker();
}

// NB: Despite other getters we need to return pointer, not a reference to pointer.
IInvokerPtr TBootstrap::GetStorageLookupInvoker() const
{
    return StorageLookupThreadPool_->GetInvoker("default");
}

const IInvokerPtr& TBootstrap::GetJobThrottlerInvoker() const
{
    return JobThrottlerQueue_->GetInvoker();
}

const NNative::IClientPtr& TBootstrap::GetMasterClient() const
{
    return MasterClient_;
}

const NNative::IConnectionPtr& TBootstrap::GetMasterConnection() const
{
    return MasterConnection_;
}

const IServerPtr& TBootstrap::GetRpcServer() const
{
    return RpcServer_;
}

const IMapNodePtr& TBootstrap::GetOrchidRoot() const
{
    return OrchidRoot_;
}

const TJobControllerPtr& TBootstrap::GetJobController() const
{
    return JobController_;
}

const TJobReporterPtr& TBootstrap::GetJobReporter() const
{
    return JobReporter_;
}

const NTabletNode::TSlotManagerPtr& TBootstrap::GetTabletSlotManager() const
{
    return TabletSlotManager_;
}

const TSecurityManagerPtr& TBootstrap::GetSecurityManager() const
{
    return SecurityManager_;
}

const IInMemoryManagerPtr& TBootstrap::GetInMemoryManager() const
{
    return InMemoryManager_;
}

const TVersionedChunkMetaManagerPtr& TBootstrap::GetVersionedChunkMetaManager() const
{
    return VersionedChunkMetaManager_;
}

const NExecAgent::TSlotManagerPtr& TBootstrap::GetExecSlotManager() const
{
    return ExecSlotManager_;
}

const NJobAgent::TGpuManagerPtr& TBootstrap::GetGpuManager() const
{
    return GpuManager_;
}

const TChunkStorePtr& TBootstrap::GetChunkStore() const
{
    return ChunkStore_;
}

const TChunkCachePtr& TBootstrap::GetChunkCache() const
{
    return ChunkCache_;
}

const TNodeMemoryTrackerPtr& TBootstrap::GetMemoryUsageTracker() const
{
    return MemoryUsageTracker_;
}

const TChunkRegistryPtr& TBootstrap::GetChunkRegistry() const
{
    return ChunkRegistry_;
}

const TSessionManagerPtr& TBootstrap::GetSessionManager() const
{
    return SessionManager_;
}

const TChunkBlockManagerPtr& TBootstrap::GetChunkBlockManager() const
{
    return ChunkBlockManager_;
}

TNetworkStatistics& TBootstrap::GetNetworkStatistics() const
{
    return *NetworkStatistics_;
}

const TChunkMetaManagerPtr& TBootstrap::GetChunkMetaManager() const
{
    return ChunkMetaManager_;
}

const IBlockCachePtr& TBootstrap::GetBlockCache() const
{
    return BlockCache_;
}

const TBlockMetaCachePtr& TBootstrap::GetBlockMetaCache() const
{
    return BlockMetaCache_;
}

const TPeerBlockDistributorPtr& TBootstrap::GetPeerBlockDistributor() const
{
    return PeerBlockDistributor_;
}

const TPeerBlockTablePtr& TBootstrap::GetPeerBlockTable() const
{
    return PeerBlockTable_;
}

const TPeerBlockUpdaterPtr& TBootstrap::GetPeerBlockUpdater() const
{
    return PeerBlockUpdater_;
}

const TBlobReaderCachePtr& TBootstrap::GetBlobReaderCache() const
{
    return BlobReaderCache_;
}

const TTableSchemaCachePtr& TBootstrap::GetTableSchemaCache() const
{
    return TableSchemaCache_;
}

const TJournalDispatcherPtr& TBootstrap::GetJournalDispatcher() const
{
    return JournalDispatcher_;
}

const TMasterConnectorPtr& TBootstrap::GetMasterConnector() const
{
    return MasterConnector_;
}

const TNodeDirectoryPtr& TBootstrap::GetNodeDirectory() const
{
    return MasterConnection_->GetNodeDirectory();
}

const TDynamicConfigManagerPtr& TBootstrap::GetDynamicConfigManager() const
{
    return DynamicConfigManager_;
}

const TNodeResourceManagerPtr& TBootstrap::GetNodeResourceManager() const
{
    return NodeResourceManager_;
}

const IQuerySubexecutorPtr& TBootstrap::GetQueryExecutor() const
{
    return QueryExecutor_;
}

TCellId TBootstrap::GetCellId() const
{
    return Config_->ClusterConnection->PrimaryMaster->CellId;
}

TCellId TBootstrap::GetCellId(TCellTag cellTag) const
{
    return cellTag == PrimaryMasterCellTag
        ? GetCellId()
        : ReplaceCellTagInId(GetCellId(), cellTag);
}

const NQueryClient::TColumnEvaluatorCachePtr& TBootstrap::GetColumnEvaluatorCache() const
{
    return ColumnEvaluatorCache_;
}

const IThroughputThrottlerPtr& TBootstrap::GetReplicationInThrottler() const
{
    return ReplicationInThrottler_;
}

const IThroughputThrottlerPtr& TBootstrap::GetReplicationOutThrottler() const
{
    return ReplicationOutThrottler_;
}

const IThroughputThrottlerPtr& TBootstrap::GetRepairInThrottler() const
{
    return RepairInThrottler_;
}

const IThroughputThrottlerPtr& TBootstrap::GetRepairOutThrottler() const
{
    return RepairOutThrottler_;
}

const IThroughputThrottlerPtr& TBootstrap::GetArtifactCacheInThrottler() const
{
    return ArtifactCacheInThrottler_;
}

const IThroughputThrottlerPtr& TBootstrap::GetArtifactCacheOutThrottler() const
{
    return ArtifactCacheOutThrottler_;
}

const IThroughputThrottlerPtr& TBootstrap::GetSkynetOutThrottler() const
{
    return SkynetOutThrottler_;
}

const IThroughputThrottlerPtr& TBootstrap::GetInThrottler(const TWorkloadDescriptor& descriptor) const
{
    switch (descriptor.Category) {
        case EWorkloadCategory::SystemRepair:
            return RepairInThrottler_;

        case EWorkloadCategory::SystemReplication:
            return ReplicationInThrottler_;

        case EWorkloadCategory::SystemArtifactCacheDownload:
            return ArtifactCacheInThrottler_;

        case EWorkloadCategory::SystemTabletCompaction:
        case EWorkloadCategory::SystemTabletPartitioning:
            return DataNodeTabletCompactionAndPartitioningInThrottler_;

        case EWorkloadCategory::SystemTabletLogging:
            return DataNodeTabletLoggingInThrottler_;

        case EWorkloadCategory::SystemTabletSnapshot:
            return DataNodeTabletSnapshotInThrottler_;

        case EWorkloadCategory::SystemTabletStoreFlush:
            return DataNodeTabletStoreFlushInThrottler_;

        default:
            return TotalInThrottler_;
    }
}

const IThroughputThrottlerPtr& TBootstrap::GetOutThrottler(const TWorkloadDescriptor& descriptor) const
{
    switch (descriptor.Category) {
        case EWorkloadCategory::SystemRepair:
            return RepairOutThrottler_;

        case EWorkloadCategory::SystemReplication:
            return ReplicationOutThrottler_;

        case EWorkloadCategory::SystemArtifactCacheDownload:
            return ArtifactCacheOutThrottler_;

        case EWorkloadCategory::SystemTabletCompaction:
        case EWorkloadCategory::SystemTabletPartitioning:
            return DataNodeTabletCompactionAndPartitioningOutThrottler_;

        case EWorkloadCategory::SystemTabletPreload:
            return DataNodeTabletPreloadOutThrottler_;

        case EWorkloadCategory::SystemTabletRecovery:
            return DataNodeTabletRecoveryOutThrottler_;

        case EWorkloadCategory::SystemTabletReplication:
            return DataNodeTabletReplicationOutThrottler_;

        default:
            return TotalOutThrottler_;
    }
}

const IThroughputThrottlerPtr& TBootstrap::GetTabletNodeInThrottler(EWorkloadCategory category) const
{
    switch (category) {
        case EWorkloadCategory::SystemTabletCompaction:
        case EWorkloadCategory::SystemTabletPartitioning:
            return TabletNodeCompactionAndPartitioningInThrottler_;

        case EWorkloadCategory::SystemTabletPreload:
            return TabletNodePreloadInThrottler_;

        case EWorkloadCategory::SystemTabletReplication:
            return TabletNodeTabletReplicationInThrottler_;

        default:
            return TotalInThrottler_;
    }
}

const IThroughputThrottlerPtr& TBootstrap::GetTabletNodeOutThrottler(EWorkloadCategory category) const
{
    switch (category) {
        case EWorkloadCategory::SystemTabletCompaction:
        case EWorkloadCategory::SystemTabletPartitioning:
            return TabletNodeCompactionAndPartitioningOutThrottler_;

        case EWorkloadCategory::SystemTabletStoreFlush:
            return TabletNodeStoreFlushOutThrottler_;

        case EWorkloadCategory::SystemTabletReplication:
            return TabletNodeTabletReplicationOutThrottler_;

        default:
            return TotalOutThrottler_;
    }
}

const IThroughputThrottlerPtr& TBootstrap::GetReadRpsOutThrottler() const
{
    return ReadRpsOutThrottler_;
}

TNetworkPreferenceList TBootstrap::GetLocalNetworks()
{
    return Config_->Addresses.empty()
        ? DefaultNetworkPreferences
        : GetIths<0>(Config_->Addresses);
}

std::optional<TString> TBootstrap::GetDefaultNetworkName()
{
    return Config_->BusServer->DefaultNetwork;
}

EJobEnvironmentType TBootstrap::GetEnvironmentType() const
{
    return ConvertTo<EJobEnvironmentType>(Config_->ExecAgent->SlotManager->JobEnvironment->AsMap()->FindChild("type"));
}

const std::vector<TIP6Address>& TBootstrap::GetResolvedNodeAddresses() const
{
    return ResolvedNodeAddresses_;
}

TJobProxyConfigPtr TBootstrap::BuildJobProxyConfig() const
{
    auto proxyConfig = CloneYsonSerializable(JobProxyConfigTemplate_);
    auto localDescriptor = GetMasterConnector()->GetLocalDescriptor();
    proxyConfig->DataCenter = localDescriptor.GetDataCenter();
    proxyConfig->Rack = localDescriptor.GetRack();
    proxyConfig->Addresses = localDescriptor.Addresses();
    return proxyConfig;
}

TTimestamp TBootstrap::GetLatestTimestamp() const
{
    return MasterConnection_
        ->GetTimestampProvider()
        ->GetLatestTimestamp();
}

void TBootstrap::PopulateAlerts(std::vector<TError>* alerts)
{
    // NB: Don't expect IsXXXExceeded helpers to be atomic.
    auto totalUsed = MemoryUsageTracker_->GetTotalUsed();
    auto totalLimit = MemoryUsageTracker_->GetTotalLimit();
    if (totalUsed > totalLimit) {
        alerts->push_back(TError("Total memory limit exceeded")
            << TErrorAttribute("used", totalUsed)
            << TErrorAttribute("limit", totalLimit));
    }

    for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
        auto used = MemoryUsageTracker_->GetUsed(category);
        auto limit = MemoryUsageTracker_->GetLimit(category);
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
    for (const auto& masterCacheService : MasterCacheServices_) {
        RpcServer_->RegisterService(masterCacheService);
    }
}

void TBootstrap::OnMasterDisconnected()
{
    for (const auto& masterCacheService : MasterCacheServices_) {
        RpcServer_->UnregisterService(masterCacheService);
    }
}

void TBootstrap::OnDynamicConfigUpdated(TCellNodeDynamicConfigPtr newConfig)
{
    Y_UNUSED(newConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellNode
