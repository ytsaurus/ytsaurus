#include "bootstrap.h"
#include "config.h"
#include "batching_chunk_service.h"
#include "dynamic_config_manager.h"
#include "node_resource_manager.h"
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

#include <yt/server/node/query_agent/query_executor.h>
#include <yt/server/node/query_agent/query_service.h>

#include <yt/server/node/tablet_node/backing_store_cleaner.h>
#include <yt/server/node/tablet_node/hint_manager.h>
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

#ifdef __linux__
#include <yt/server/lib/containers/instance.h>
#include <yt/server/lib/containers/instance_limits_tracker.h>
#include <yt/server/lib/containers/porto_executor.h>
#endif

#include <yt/server/lib/core_dump/core_dumper.h>

#include <yt/server/lib/hydra/snapshot.h>
#include <yt/server/lib/hydra/file_snapshot_store.h>

#include <yt/ytlib/program/build_attributes.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/ytlib/hydra/peer_channel.h>

#include <yt/ytlib/hive/cell_directory_synchronizer.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/ytlib/object_client/caching_object_service.h>
#include <yt/ytlib/object_client/object_service_cache.h>
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
#include <yt/core/concurrency/spinlock.h>

#include <yt/core/net/address.h>

#include <yt/core/misc/collection_helpers.h>
#include <yt/core/misc/core_dumper.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/core/ytalloc/statistics_producer.h>
#include <yt/core/ytalloc/bindings.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/rpc/bus/channel.h>
#include <yt/core/rpc/bus/server.h>
#include <yt/core/rpc/caching_channel_factory.h>
#include <yt/core/rpc/channel.h>
#include <yt/core/rpc/server.h>
#include <yt/core/rpc/dispatcher.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/virtual.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

namespace NYT::NClusterNode {

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
using namespace NTableClient;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TClusterNodeConfigPtr config, INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
    , DynamicConfig_(New<TClusterNodeDynamicConfig>())
{ }

TBootstrap::~TBootstrap() = default;

void TBootstrap::Initialize()
{
    srand(time(nullptr));

    ControlActionQueue_ = New<TActionQueue>("Control");
    JobActionQueue_ = New<TActionQueue>("Job");

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
    // TOOD(gritukan): Make node without dynamic config read-only after YT-12933.
    return false;
}

void TBootstrap::DoInitialize()
{
    auto localRpcAddresses = GetLocalAddresses(Config_->Addresses, Config_->RpcPort);

    if (!Config_->ClusterConnection->Networks) {
        Config_->ClusterConnection->Networks = GetLocalNetworks();
    }

    YT_LOG_INFO("Initializing node (LocalAddresses: %v, PrimaryMasterAddresses: %v, NodeTags: %v)",
        GetValues(localRpcAddresses),
        Config_->ClusterConnection->PrimaryMaster->Addresses,
        Config_->Tags);

    NodeResourceManager_ = New<TNodeResourceManager>(this);

#ifdef __linux__
    if (GetEnvironmentType() == EJobEnvironmentType::Porto) {
        auto portoEnvironmentConfig = ConvertTo<TPortoJobEnvironmentConfigPtr>(Config_->ExecAgent->SlotManager->JobEnvironment);
        auto portoExecutor = CreatePortoExecutor(
            portoEnvironmentConfig->PortoExecutor,
            "limits_tracker");

        portoExecutor->SubscribeFailed(BIND([=] (const TError& error) {
            YT_LOG_ERROR(error, "Porto executor failed");
            auto slotManager = GetExecSlotManager();
            if (slotManager) {
                slotManager->Disable(error);
            }
        }));

        auto self = GetSelfPortoInstance(portoExecutor);
        if (Config_->InstanceLimitsUpdatePeriod) {
            auto instance = portoEnvironmentConfig->UseDaemonSubcontainer
                ? GetPortoInstance(portoExecutor, self->GetParentName())
                : self;

            InstanceLimitsTracker_ = New<TInstanceLimitsTracker>(
                instance,
                GetControlInvoker(),
                *Config_->InstanceLimitsUpdatePeriod);

            InstanceLimitsTracker_->SubscribeLimitsUpdated(BIND(&TNodeResourceManager::OnInstanceLimitsUpdated, NodeResourceManager_)
                .Via(GetControlInvoker()));
        }

        if (portoEnvironmentConfig->UseDaemonSubcontainer) {
            self->SetCpuWeight(Config_->ResourceLimits->NodeCpuWeight);

            NodeResourceManager_->SubscribeSelfMemoryGuaranteeUpdated(BIND([self] (i64 memoryGuarantee) {
                YT_LOG_DEBUG("Self memory guarantee updated (MemoryGuarantee: %v)", memoryGuarantee);
                self->SetMemoryGuarantee(memoryGuarantee);
            }));
        }
    }
#endif

    MemoryUsageTracker_ = New<TNodeMemoryTracker>(
        Config_->ResourceLimits->TotalMemory,
        std::vector<std::pair<EMemoryCategory, i64>>{},
        Logger,
        TRegistry("/cluster_node/memory_usage"));

    MasterConnection_ = NApi::NNative::CreateConnection(Config_->ClusterConnection);
    MasterClient_ = MasterConnection_->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::RootUserName));

    MasterCacheQueue_ = New<TActionQueue>("MasterCache");
    QueryThreadPool_ = CreateTwoLevelFairShareThreadPool(
        Config_->QueryAgent->QueryThreadPoolSize,
        "Query");
    TabletLookupThreadPool_ = New<TThreadPool>(
        Config_->QueryAgent->LookupThreadPoolSize,
        "TabletLookup");
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

    BlockMetaCache_ = New<TBlockMetaCache>(Config_->DataNode->BlockMetaCache, TRegistry("/data_node/block_meta_cache"));

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

    TabletNodeHintManager_ = New<NTabletNode::THintManager>(Config_->TabletNode->HintManager, this);
    TabletSlotManager_ = New<NTabletNode::TSlotManager>(Config_->TabletNode, this);
    MasterConnector_->SubscribePopulateAlerts(BIND(&NTabletNode::TSlotManager::PopulateAlerts, TabletSlotManager_));

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    ChunkStore_ = New<NDataNode::TChunkStore>(Config_->DataNode, this);

    ChunkCache_ = New<TChunkCache>(Config_->DataNode, this);

    for (auto kind : TEnumTraits<EDataNodeThrottlerKind>::GetDomainValues()) {
        RawDataNodeThrottlers_[kind] = CreateNamedReconfigurableThroughputThrottler(
            Config_->DataNode->Throttlers[kind],
            ToString(kind),
            DataNodeLogger,
            DataNodeProfiler.WithPrefix("/throttlers"));
    }
    auto totalInThrottler = IThroughputThrottlerPtr(RawDataNodeThrottlers_[EDataNodeThrottlerKind::TotalIn]);
    auto totalOutThrottler = IThroughputThrottlerPtr(RawDataNodeThrottlers_[EDataNodeThrottlerKind::TotalOut]);
    static const THashSet<EDataNodeThrottlerKind> InCombinedDataNodeThrottlerKinds = {
        EDataNodeThrottlerKind::ReplicationIn,
        EDataNodeThrottlerKind::RepairIn,
        EDataNodeThrottlerKind::ArtifactCacheIn,
        EDataNodeThrottlerKind::TabletCompactionAndPartitioningIn,
        EDataNodeThrottlerKind::TabletCompactionAndPartitioningIn,
        EDataNodeThrottlerKind::TabletLoggingIn,
        EDataNodeThrottlerKind::TabletSnapshotIn,
        EDataNodeThrottlerKind::TabletStoreFlushIn,
        EDataNodeThrottlerKind::JobIn,
    };
    static const THashSet<EDataNodeThrottlerKind> OutCombinedDataNodeThrottlerKinds = {
        EDataNodeThrottlerKind::ReplicationOut,
        EDataNodeThrottlerKind::RepairOut,
        EDataNodeThrottlerKind::ArtifactCacheOut,
        EDataNodeThrottlerKind::TabletCompactionAndPartitioningOut,
        EDataNodeThrottlerKind::SkynetOut,
        EDataNodeThrottlerKind::TabletCompactionAndPartitioningOut,
        EDataNodeThrottlerKind::TabletPreloadOut,
        EDataNodeThrottlerKind::TabletRecoveryOut,
        EDataNodeThrottlerKind::TabletReplicationOut,
        EDataNodeThrottlerKind::JobOut,
    };
    for (auto kind : TEnumTraits<EDataNodeThrottlerKind>::GetDomainValues()) {
        auto throttler = IThroughputThrottlerPtr(RawDataNodeThrottlers_[kind]);
        if (InCombinedDataNodeThrottlerKinds.contains(kind)) {
            throttler = CreateCombinedThrottler({totalInThrottler, throttler});
        }
        if (OutCombinedDataNodeThrottlerKinds.contains(kind)) {
            throttler = CreateCombinedThrottler({totalOutThrottler, throttler});
        }
        DataNodeThrottlers_[kind] = throttler;
    }

    for (auto kind : TEnumTraits<ETabletNodeThrottlerKind>::GetDomainValues()) {
        RawTabletNodeThrottlers_[kind] = CreateNamedReconfigurableThroughputThrottler(
            Config_->TabletNode->Throttlers[kind],
            ToString(kind),
            TabletNodeLogger,
            TabletNodeProfiler.WithPrefix("/throttlers"));
    }
    static const THashSet<ETabletNodeThrottlerKind> InCombinedTabletNodeThrottlerKinds = {
        ETabletNodeThrottlerKind::StoreCompactionAndPartitioningIn,
        ETabletNodeThrottlerKind::ReplicationIn,
        ETabletNodeThrottlerKind::StaticStorePreloadIn
    };
    static const THashSet<ETabletNodeThrottlerKind> OutCombinedTabletNodeThrottlerKinds = {
        ETabletNodeThrottlerKind::StoreCompactionAndPartitioningOut,
        ETabletNodeThrottlerKind::StoreFlushOut,
        ETabletNodeThrottlerKind::ReplicationOut,
        ETabletNodeThrottlerKind::DynamicStoreReadOut
    };
    for (auto kind : TEnumTraits<ETabletNodeThrottlerKind>::GetDomainValues()) {
        auto throttler = IThroughputThrottlerPtr(RawTabletNodeThrottlers_[kind]);
        if (InCombinedTabletNodeThrottlerKinds.contains(kind)) {
            throttler = CreateCombinedThrottler({totalInThrottler, throttler});
        }
        if (OutCombinedTabletNodeThrottlerKinds.contains(kind)) {
            throttler = CreateCombinedThrottler({totalOutThrottler, throttler});
        }
        TabletNodeThrottlers_[kind] = throttler;
    }

    DynamicConfigManager_ = New<TClusterNodeDynamicConfigManager>(this);
    DynamicConfigManager_->SubscribeConfigUpdated(BIND(&TBootstrap::OnDynamicConfigUpdated, this));

    RpcServer_->RegisterService(CreateDataNodeService(Config_->DataNode, this));

    RpcServer_->RegisterService(CreateInMemoryService(Config_->TabletNode->InMemoryManager, this));

    auto localAddress = GetDefaultAddress(localRpcAddresses);

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
    JobProxyConfigTemplate_->StderrPath = Config_->ExecAgent->JobProxyStderrPath;
    JobProxyConfigTemplate_->TestRootFS = Config_->ExecAgent->TestRootFS;

    JobProxyConfigTemplate_->CoreWatcher = Config_->ExecAgent->CoreWatcher;

    JobProxyConfigTemplate_->TestPollJobShell = Config_->ExecAgent->TestPollJobShell;

    JobProxyConfigTemplate_->DoNotSetUserId = Config_->ExecAgent->DoNotSetUserId;

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

    SecurityManager_ = New<TSecurityManager>(Config_->TabletNode->SecurityManager, this);

    InMemoryManager_ = CreateInMemoryManager(Config_->TabletNode->InMemoryManager, this);

    VersionedChunkMetaManager_ = New<TVersionedChunkMetaManager>(Config_->TabletNode, this);

    QueryExecutor_ = CreateQuerySubexecutor(Config_->QueryAgent, this);

    RpcServer_->RegisterService(CreateQueryService(Config_->QueryAgent, this));

    auto timestampProviderConfig = Config_->TimestampProvider;
    if (!timestampProviderConfig) {
        timestampProviderConfig = CreateRemoteTimestampProviderConfig(Config_->ClusterConnection->PrimaryMaster);
    }
    auto timestampProvider = CreateBatchingRemoteTimestampProvider(
        timestampProviderConfig,
        CreateTimestampProviderChannel(timestampProviderConfig, MasterConnection_->GetChannelFactory()));
    RpcServer_->RegisterService(CreateTimestampProxyService(timestampProvider));

    ObjectServiceCache_ = New<TObjectServiceCache>(
        Config_->CachingObjectService,
        Logger,
        TRegistry("/cluster_node/master_cache"));

    {
        auto result = GetMemoryUsageTracker()->TryAcquire(EMemoryCategory::MasterCache, Config_->CachingObjectService->Capacity);
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error reserving memory for master cache");
    }

    auto initCachingObjectService = [&] (const auto& masterConfig) {
        return CreateCachingObjectService(
            Config_->CachingObjectService,
            MasterCacheQueue_->GetInvoker(),
            CreateDefaultTimeoutChannel(
                CreatePeerChannel(
                    masterConfig,
                    MasterConnection_->GetChannelFactory(),
                    EPeerKind::Follower),
                masterConfig->RpcTimeout),
            ObjectServiceCache_,
            masterConfig->CellId,
            Logger);
    };

    CachingObjectServices_.push_back(initCachingObjectService(
        Config_->ClusterConnection->PrimaryMaster));

    for (const auto& masterConfig : Config_->ClusterConnection->SecondaryMasters) {
        CachingObjectServices_.push_back(initCachingObjectService(masterConfig));
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
    auto localRpcAddresses = GetLocalAddresses(Config_->Addresses, Config_->RpcPort);

    YT_LOG_INFO("Starting node (LocalAddresses: %v, PrimaryMasterAddresses: %v, NodeTags: %v)",
        GetValues(localRpcAddresses),
        Config_->ClusterConnection->PrimaryMaster->Addresses,
        Config_->Tags);

    DynamicConfigManager_->Start();
    TabletNodeHintManager_->Start();

    NodeResourceManager_->Start();
#ifdef __linux__
    if (InstanceLimitsTracker_) {
        InstanceLimitsTracker_->Start();
    }
#endif

    // Force start node directory synchronizer.
    MasterConnection_->GetNodeDirectorySynchronizer()->Start();

    Config_->MonitoringServer->Port = Config_->MonitoringPort;
    Config_->MonitoringServer->BindRetryCount = Config_->BusServer->BindRetryCount;
    Config_->MonitoringServer->BindRetryBackoff = Config_->BusServer->BindRetryBackoff;
    Config_->MonitoringServer->ServerName = "monitoring";
    HttpServer_ = NHttp::CreateServer(Config_->MonitoringServer);

    auto skynetHttpConfig = New<NHttp::TServerConfig>();
    skynetHttpConfig->Port = Config_->SkynetHttpPort;
    skynetHttpConfig->BindRetryCount = Config_->BusServer->BindRetryCount;
    skynetHttpConfig->BindRetryBackoff = Config_->BusServer->BindRetryBackoff;
    skynetHttpConfig->ServerName = "skynet";
    SkynetHttpServer_ = NHttp::CreateServer(skynetHttpConfig);

    NMonitoring::Initialize(HttpServer_, &MonitoringManager_, &OrchidRoot_, Config_->SolomonExporter);

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
    SetNodeByYPath(
        OrchidRoot_,
        "/object_service_cache",
        CreateVirtualNode(ObjectServiceCache_->GetOrchidService()
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
    StartBackingStoreCleaner(Config_->TabletNode, this);

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

const TClusterNodeConfigPtr& TBootstrap::GetConfig() const
{
    return Config_;
}

const TClusterNodeDynamicConfigPtr& TBootstrap::GetDynamicConfig() const
{
    return DynamicConfig_;
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlActionQueue_->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetJobInvoker() const
{
    return JobActionQueue_->GetInvoker();
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

const NTabletNode::THintManagerPtr& TBootstrap::GetTabletNodeHintManager() const
{
    return TabletNodeHintManager_;
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

const TClusterNodeDynamicConfigManagerPtr& TBootstrap::GetDynamicConfigManager() const
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

std::vector<TString> TBootstrap::GetMasterAddressesOrThrow(TCellTag cellTag) const
{
    auto cellId = GetCellId(cellTag);

    if (Config_->ClusterConnection->PrimaryMaster->CellId == cellId) {
        return Config_->ClusterConnection->PrimaryMaster->Addresses;
    }

    for (const auto& secondaryMaster : Config_->ClusterConnection->SecondaryMasters) {
        if (secondaryMaster->CellId == cellId) {
            return secondaryMaster->Addresses;
        }
    }

    THROW_ERROR_EXCEPTION("Master with cell tag %v is not known", cellTag);
}

const NQueryClient::TColumnEvaluatorCachePtr& TBootstrap::GetColumnEvaluatorCache() const
{
    return ColumnEvaluatorCache_;
}

const IThroughputThrottlerPtr& TBootstrap::GetDataNodeThrottler(EDataNodeThrottlerKind kind) const
{
    return DataNodeThrottlers_[kind];
}

const IThroughputThrottlerPtr& TBootstrap::GetTabletNodeThrottler(ETabletNodeThrottlerKind kind) const
{
    return TabletNodeThrottlers_[kind];
}

const IThroughputThrottlerPtr& TBootstrap::GetDataNodeInThrottler(const TWorkloadDescriptor& descriptor) const
{
    static const THashMap<EWorkloadCategory, EDataNodeThrottlerKind> WorkloadCategoryToThrottlerKind = {
        {EWorkloadCategory::SystemRepair,                EDataNodeThrottlerKind::RepairIn},
        {EWorkloadCategory::SystemReplication,           EDataNodeThrottlerKind::ReplicationIn},
        {EWorkloadCategory::SystemArtifactCacheDownload, EDataNodeThrottlerKind::ArtifactCacheIn},
        {EWorkloadCategory::SystemTabletCompaction,      EDataNodeThrottlerKind::TabletCompactionAndPartitioningIn},
        {EWorkloadCategory::SystemTabletPartitioning,    EDataNodeThrottlerKind::TabletCompactionAndPartitioningIn},
        {EWorkloadCategory::SystemTabletLogging,         EDataNodeThrottlerKind::TabletLoggingIn},
        {EWorkloadCategory::SystemTabletSnapshot,        EDataNodeThrottlerKind::TabletSnapshotIn},
        {EWorkloadCategory::SystemTabletStoreFlush,      EDataNodeThrottlerKind::TabletStoreFlushIn}
    };
    auto it = WorkloadCategoryToThrottlerKind.find(descriptor.Category);
    return it == WorkloadCategoryToThrottlerKind.end()
        ? DataNodeThrottlers_[EDataNodeThrottlerKind::TotalIn]
        : DataNodeThrottlers_[it->second];
}

const IThroughputThrottlerPtr& TBootstrap::GetDataNodeOutThrottler(const TWorkloadDescriptor& descriptor) const
{
    static const THashMap<EWorkloadCategory, EDataNodeThrottlerKind> WorkloadCategoryToThrottlerKind = {
        {EWorkloadCategory::SystemRepair,                EDataNodeThrottlerKind::RepairOut},
        {EWorkloadCategory::SystemReplication,           EDataNodeThrottlerKind::ReplicationOut},
        {EWorkloadCategory::SystemArtifactCacheDownload, EDataNodeThrottlerKind::ArtifactCacheOut},
        {EWorkloadCategory::SystemTabletCompaction,      EDataNodeThrottlerKind::TabletCompactionAndPartitioningOut},
        {EWorkloadCategory::SystemTabletPartitioning,    EDataNodeThrottlerKind::TabletCompactionAndPartitioningOut},
        {EWorkloadCategory::SystemTabletPreload,         EDataNodeThrottlerKind::TabletPreloadOut},
        {EWorkloadCategory::SystemTabletRecovery,        EDataNodeThrottlerKind::TabletRecoveryOut},
        {EWorkloadCategory::SystemTabletReplication,     EDataNodeThrottlerKind::TabletReplicationOut}
    };
    auto it = WorkloadCategoryToThrottlerKind.find(descriptor.Category);
    return it == WorkloadCategoryToThrottlerKind.end()
        ? DataNodeThrottlers_[EDataNodeThrottlerKind::TotalOut]
        : DataNodeThrottlers_[it->second];
}

const IThroughputThrottlerPtr& TBootstrap::GetTabletNodeInThrottler(EWorkloadCategory category) const
{
    static const THashMap<EWorkloadCategory, ETabletNodeThrottlerKind> WorkloadCategoryToThrottlerKind = {
        {EWorkloadCategory::SystemTabletCompaction,      ETabletNodeThrottlerKind::StoreCompactionAndPartitioningIn},
        {EWorkloadCategory::SystemTabletPartitioning,    ETabletNodeThrottlerKind::StoreCompactionAndPartitioningIn},
        {EWorkloadCategory::SystemTabletPreload,         ETabletNodeThrottlerKind::StaticStorePreloadIn},
    };
    auto it = WorkloadCategoryToThrottlerKind.find(category);
    return it ==  WorkloadCategoryToThrottlerKind.end()
        ? DataNodeThrottlers_[EDataNodeThrottlerKind::TotalIn]
        : TabletNodeThrottlers_[it->second];
}

const IThroughputThrottlerPtr& TBootstrap::GetTabletNodeOutThrottler(EWorkloadCategory category) const
{
    static const THashMap<EWorkloadCategory, ETabletNodeThrottlerKind> WorkloadCategoryToThrottlerKind = {
        {EWorkloadCategory::SystemTabletCompaction,      ETabletNodeThrottlerKind::StoreCompactionAndPartitioningOut},
        {EWorkloadCategory::SystemTabletPartitioning,    ETabletNodeThrottlerKind::StoreCompactionAndPartitioningOut},
        {EWorkloadCategory::SystemTabletStoreFlush,      ETabletNodeThrottlerKind::StoreFlushOut},
        {EWorkloadCategory::SystemTabletReplication,     ETabletNodeThrottlerKind::ReplicationOut},
        {EWorkloadCategory::UserDynamicStoreRead,        ETabletNodeThrottlerKind::DynamicStoreReadOut}
    };
    auto it = WorkloadCategoryToThrottlerKind.find(category);
    return it ==  WorkloadCategoryToThrottlerKind.end()
        ? DataNodeThrottlers_[EDataNodeThrottlerKind::TotalOut]
        : TabletNodeThrottlers_[it->second];
}

TNetworkPreferenceList TBootstrap::GetLocalNetworks() const
{
    return Config_->Addresses.empty()
        ? DefaultNetworkPreferences
        : GetIths<0>(Config_->Addresses);
}

std::optional<TString> TBootstrap::GetDefaultNetworkName() const
{
    return Config_->BusServer->DefaultNetwork;
}

TString TBootstrap::GetDefaultLocalAddressOrThrow() const
{
    auto addressMap = GetLocalAddresses(
        Config_->Addresses,
        Config_->RpcPort);
    auto defaultNetwork = GetDefaultNetworkName();

    if (!defaultNetwork) {
        THROW_ERROR_EXCEPTION("Default network is not configured");
    }

    if (!addressMap.contains(*defaultNetwork)) {
        THROW_ERROR_EXCEPTION("Address for the default network is not configured");
    }

    return addressMap[*defaultNetwork];
}

EJobEnvironmentType TBootstrap::GetEnvironmentType() const
{
    return ConvertTo<EJobEnvironmentType>(Config_->ExecAgent->SlotManager->JobEnvironment->AsMap()->FindChild("type"));
}

bool TBootstrap::IsSimpleEnvironment() const
{
    return GetEnvironmentType() == EJobEnvironmentType::Simple;
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
    for (const auto& cachingObjectService : CachingObjectServices_) {
        RpcServer_->RegisterService(cachingObjectService);
    }
}

void TBootstrap::OnMasterDisconnected()
{
    for (const auto& cachingObjectService : CachingObjectServices_) {
        RpcServer_->UnregisterService(cachingObjectService);
    }
}

void TBootstrap::OnDynamicConfigUpdated(const TClusterNodeDynamicConfigPtr& newConfig)
{
    DynamicConfig_ = newConfig;

    // Reconfigure spinlock profiling.
    NConcurrency::SetSpinlockHiccupThresholdTicks(NProfiling::DurationToCpuDuration(
        newConfig->SpinlockHiccupThreshold.value_or(Config_->SpinlockHiccupThreshold)));

    // Reconfigure YTAlloc (unless configured from env).
    if (!NYTAlloc::IsConfiguredFromEnv()) {
        NYTAlloc::Configure(newConfig->YTAlloc ? newConfig->YTAlloc : Config_->YTAlloc);
    }

    // Reconfigure RPC.
    NRpc::TDispatcher::Get()->Configure(Config_->RpcDispatcher->ApplyDynamic(newConfig->RpcDispatcher));

    // Reconfigure Chunk Client.
    NChunkClient::TDispatcher::Get()->Configure(Config_->ChunkClientDispatcher->ApplyDynamic(newConfig->ChunkClientDispatcher));

    // Reconfigure thread pools.
    QueryThreadPool_->Configure(
        newConfig->QueryAgent->QueryThreadPoolSize.value_or(Config_->QueryAgent->QueryThreadPoolSize));
    TabletLookupThreadPool_->Configure(
        newConfig->QueryAgent->LookupThreadPoolSize.value_or(Config_->QueryAgent->LookupThreadPoolSize));
    TableReplicatorThreadPool_->Configure(
        newConfig->TabletNode->TabletManager->ReplicatorThreadPoolSize.value_or(Config_->TabletNode->TabletManager->ReplicatorThreadPoolSize));
    StorageHeavyThreadPool_->Configure(
        newConfig->DataNode->StorageHeavyThreadCount.value_or(Config_->DataNode->StorageHeavyThreadCount));
    StorageLightThreadPool_->Configure(
        newConfig->DataNode->StorageLightThreadCount.value_or(Config_->DataNode->StorageLightThreadCount));
    StorageLookupThreadPool_->Configure(
        newConfig->DataNode->StorageLookupThreadCount.value_or(Config_->DataNode->StorageLookupThreadCount));

    // Reconfigure Data Node throttlers.
    for (auto kind : TEnumTraits<NDataNode::EDataNodeThrottlerKind>::GetDomainValues()) {
        auto throttlerConfig = newConfig->DataNode->Throttlers[kind]
            ? newConfig->DataNode->Throttlers[kind]
            : Config_->DataNode->Throttlers[kind];
        RawDataNodeThrottlers_[kind]->Reconfigure(throttlerConfig);
    }

    // Reconfigure Tablet Node throttlers.
    for (auto kind : TEnumTraits<NTabletNode::ETabletNodeThrottlerKind>::GetDomainValues()) {
        auto throttlerConfig = newConfig->TabletNode->Throttlers[kind]
            ? newConfig->TabletNode->Throttlers[kind]
            : Config_->TabletNode->Throttlers[kind];
        RawTabletNodeThrottlers_[kind]->Reconfigure(throttlerConfig);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
