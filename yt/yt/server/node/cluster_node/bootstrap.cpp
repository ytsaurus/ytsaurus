#include "bootstrap.h"

#include "arbitrage_service.h"
#include "config.h"
#include "batching_chunk_service.h"
#include "dynamic_config_manager.h"
#include "node_resource_manager.h"
#include "master_connector.h"
#include "private.h"

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/node/cellar_node/bootstrap.h>
#include <yt/yt/server/node/cellar_node/config.h>
#include <yt/yt/server/node/cellar_node/master_connector.h>

#include <yt/yt/server/node/chaos_node/bootstrap.h>
#include <yt/yt/server/node/chaos_node/slot_manager.h>

#include <yt/yt/server/node/data_node/blob_reader_cache.h>
#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/chunk_block_manager.h>
#include <yt/yt/server/node/data_node/chunk_registry.h>
#include <yt/yt/server/node/data_node/chunk_store.h>
#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/data_node_service.h>
#include <yt/yt/server/node/data_node/job.h>
#include <yt/yt/server/node/data_node/job_heartbeat_processor.h>
#include <yt/yt/server/node/data_node/journal_dispatcher.h>
#include <yt/yt/server/node/data_node/location.h>
#include <yt/yt/server/node/data_node/legacy_master_connector.h>
#include <yt/yt/server/node/data_node/master_connector.h>
#include <yt/yt/server/node/data_node/medium_updater.h>
#include <yt/yt/server/node/data_node/network_statistics.h>
#include <yt/yt/server/node/data_node/p2p_block_distributor.h>
#include <yt/yt/server/node/data_node/block_peer_table.h>
#include <yt/yt/server/node/data_node/private.h>
#include <yt/yt/server/node/data_node/session_manager.h>
#include <yt/yt/server/node/data_node/table_schema_cache.h>
#include <yt/yt/server/node/data_node/ytree_integration.h>
#include <yt/yt/server/node/data_node/chunk_meta_manager.h>
#include <yt/yt/server/node/data_node/skynet_http_handler.h>

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/job_environment.h>
#include <yt/yt/server/node/exec_node/job.h>
#include <yt/yt/server/node/exec_node/job_heartbeat_processor.h>
#include <yt/yt/server/node/exec_node/job_prober_service.h>
#include <yt/yt/server/node/exec_node/master_connector.h>
#include <yt/yt/server/node/exec_node/private.h>
#include <yt/yt/server/node/exec_node/scheduler_connector.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>
#include <yt/yt/server/node/exec_node/supervisor_service.h>

#include <yt/yt/server/node/job_agent/job_controller.h>
#include <yt/yt/server/lib/job_agent/job_reporter.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/server/node/query_agent/query_executor.h>
#include <yt/yt/server/node/query_agent/query_service.h>

#include <yt/yt/server/node/tablet_node/backing_store_cleaner.h>
#include <yt/yt/server/node/tablet_node/bootstrap.h>
#include <yt/yt/server/node/tablet_node/hint_manager.h>
#include <yt/yt/server/node/tablet_node/master_connector.h>
#include <yt/yt/server/node/tablet_node/partition_balancer.h>
#include <yt/yt/server/node/tablet_node/slot_manager.h>
#include <yt/yt/server/node/tablet_node/store_compactor.h>
#include <yt/yt/server/node/tablet_node/store_flusher.h>
#include <yt/yt/server/node/tablet_node/store_trimmer.h>
#include <yt/yt/server/node/tablet_node/hunk_chunk_sweeper.h>
#include <yt/yt/server/node/tablet_node/lsm_interop.h>
#include <yt/yt/server/node/tablet_node/structured_logger.h>
#include <yt/yt/server/node/tablet_node/tablet_cell_service.h>
#include <yt/yt/server/node/tablet_node/tablet_cell_snapshot_validator.h>
#include <yt/yt/server/node/tablet_node/versioned_chunk_meta_manager.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/server/lib/transaction_server/timestamp_proxy_service.h>

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/containers/instance.h>
#include <yt/yt/server/lib/containers/instance_limits_tracker.h>
#include <yt/yt/server/lib/containers/porto_executor.h>

#include <yt/yt/server/lib/core_dump/core_dumper.h>

#include <yt/yt/server/lib/hydra/snapshot.h>
#include <yt/yt/server/lib/hydra/file_snapshot_store.h>

#include <yt/yt/server/lib/cellar_agent/bootstrap_proxy.h>
#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>
#include <yt/yt/server/lib/cellar_agent/config.h>

#include <yt/yt/ytlib/program/build_attributes.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/monitoring/http_integration.h>
#include <yt/yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/yt/ytlib/object_client/caching_object_service.h>
#include <yt/yt/ytlib/object_client/object_service_cache.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/query_client/column_evaluator.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>
#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/transaction_client/config.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>
#include <yt/yt/client/transaction_client/remote_timestamp_provider.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/core_dumper.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/yt/core/ytalloc/statistics_producer.h>
#include <yt/yt/core/ytalloc/bindings.h>

#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/channel.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/virtual.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

namespace NYT::NClusterNode {

using namespace NAdmin;
using namespace NApi;
using namespace NBus;
using namespace NCellarAgent;
using namespace NCellarClient;
using namespace NChaosNode;
using namespace NChunkClient;
using namespace NContainers;
using namespace NNodeTrackerClient;
using namespace NConcurrency;
using namespace NDataNode;
using namespace NElection;
using namespace NExecNode;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NHydra;
using namespace NJobAgent;
using namespace NJobProxy;
using namespace NMonitoring;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NObjectClient;
using namespace NOrchid;
using namespace NProfiling;
using namespace NQueryAgent;
using namespace NRpc;
using namespace NScheduler;
using namespace NTableClient;
using namespace NTabletNode;
using namespace NTransactionServer;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NNet;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TClusterNodeConfigPtr config, INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
{ }

TBootstrap::~TBootstrap()
{ }

void TBootstrap::Initialize()
{
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

const IMasterConnectorPtr& TBootstrap::GetMasterConnector() const
{
    return MasterConnector_;
}

TRelativeThroughputThrottlerConfigPtr TBootstrap::PatchRelativeNetworkThrottlerConfig(
    const TRelativeThroughputThrottlerConfigPtr& config) const
{
    // NB: Absolute value limit suppresses relative one.
    if (config->Limit || !config->RelativeLimit) {
        return config;
    }

    auto patchedConfig = CloneYsonSerializable(config);
    patchedConfig->Limit = *config->RelativeLimit * Config_->NetworkBandwidth;

    return patchedConfig;
}

void TBootstrap::SetDecommissioned(bool decommissioned)
{
    Decommissioned_ = decommissioned;
}

// IBootstrapBase implementation.
const TNodeMemoryTrackerPtr& TBootstrap::GetMemoryUsageTracker() const
{
    return MemoryUsageTracker_;
}

const TNodeResourceManagerPtr& TBootstrap::GetNodeResourceManager() const
{
    return NodeResourceManager_;
}

const NConcurrency::IThroughputThrottlerPtr& TBootstrap::GetTotalInThrottler() const
{
    return TotalInThrottler_;
}

const NConcurrency::IThroughputThrottlerPtr& TBootstrap::GetTotalOutThrottler() const
{
    return TotalOutThrottler_;
}

const NConcurrency::IThroughputThrottlerPtr& TBootstrap::GetReadRpsOutThrottler() const
{
    return ReadRpsOutThrottler_;
}

const TClusterNodeConfigPtr& TBootstrap::GetConfig() const
{
    return Config_;
}

const NClusterNode::TClusterNodeDynamicConfigManagerPtr& TBootstrap::GetDynamicConfigManager() const
{
    return DynamicConfigManager_;
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlActionQueue_->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetJobInvoker() const
{
    return JobActionQueue_->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetMasterConnectionInvoker() const
{
    return MasterConnector_->GetMasterConnectionInvoker();
}

const IInvokerPtr& TBootstrap::GetStorageLightInvoker() const
{
    return StorageLightThreadPool_->GetInvoker();
}

const IPrioritizedInvokerPtr& TBootstrap::GetStorageHeavyInvoker() const
{
    return StorageHeavyInvoker_;
}

const NApi::NNative::IClientPtr& TBootstrap::GetMasterClient() const
{
    return MasterClient_;
}

const NApi::NNative::IConnectionPtr& TBootstrap::GetMasterConnection() const
{
    return MasterConnection_;
}

IChannelPtr TBootstrap::GetMasterChannel(TCellTag cellTag)
{
    return MasterConnector_->GetMasterChannel(cellTag);
}

TNodeDescriptor TBootstrap::GetLocalDescriptor() const
{
    return MasterConnector_->GetLocalDescriptor();
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

const TCellTagList& TBootstrap::GetMasterCellTags() const
{
    return MasterConnector_->GetMasterCellTags();
}

std::vector<TString> TBootstrap::GetMasterAddressesOrThrow(TCellTag cellTag) const
{
    // TODO(babenko): handle service discovery.
    auto unwrapAddresses = [&] (const auto& optionalAddresses) {
        if (!optionalAddresses) {
            THROW_ERROR_EXCEPTION("Missing addresses for master cell with tag %v", cellTag);
        }
        return *optionalAddresses;
    };

    auto cellId = GetCellId(cellTag);

    if (Config_->ClusterConnection->PrimaryMaster->CellId == cellId) {
        return unwrapAddresses(Config_->ClusterConnection->PrimaryMaster->Addresses);
    }

    for (const auto& secondaryMaster : Config_->ClusterConnection->SecondaryMasters) {
        if (secondaryMaster->CellId == cellId) {
            return unwrapAddresses(secondaryMaster->Addresses);
        }
    }

    THROW_ERROR_EXCEPTION("Master with cell tag %v is not known", cellTag);
}

const TLegacyMasterConnectorPtr& TBootstrap::GetLegacyMasterConnector() const
{
    return LegacyMasterConnector_;
}

bool TBootstrap::UseNewHeartbeats() const
{
    return MasterConnector_->UseNewHeartbeats();
}

void TBootstrap::ResetAndRegisterAtMaster()
{
    return MasterConnector_->ResetAndRegisterAtMaster();
}

bool TBootstrap::IsConnected() const
{
    return MasterConnector_->IsConnected();
}

TNodeId TBootstrap::GetNodeId() const
{
    return MasterConnector_->GetNodeId();
}

TString TBootstrap::GetLocalHostName() const
{
    return MasterConnector_->GetLocalHostName();
}

TMasterEpoch TBootstrap::GetMasterEpoch() const
{
    return MasterConnector_->GetEpoch();
}

const TNodeDirectoryPtr& TBootstrap::GetNodeDirectory() const
{
    return MasterConnection_->GetNodeDirectory();
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

const NHttp::IServerPtr& TBootstrap::GetHttpServer() const
{
    return HttpServer_;
}

const NRpc::IServerPtr& TBootstrap::GetRpcServer() const
{
    return RpcServer_;
}

const IBlockCachePtr& TBootstrap::GetBlockCache() const
{
    return BlockCache_;
}

const IClientBlockCachePtr& TBootstrap::GetClientBlockCache() const
{
    return ClientBlockCache_;
}

const IChunkMetaManagerPtr& TBootstrap::GetChunkMetaManager() const
{
    return ChunkMetaManager_;
}

const IVersionedChunkMetaManagerPtr& TBootstrap::GetVersionedChunkMetaManager() const
{
    return VersionedChunkMetaManager_;
}

const NYTree::IMapNodePtr& TBootstrap::GetOrchidRoot() const
{
    return OrchidRoot_;
}

bool TBootstrap::IsReadOnly() const
{
    // TOOD(gritukan): Make node without dynamic config read-only after YT-12933.
    return false;
}

bool TBootstrap::Decommissioned() const
{
    return Decommissioned_;
}

NDataNode::TNetworkStatistics& TBootstrap::GetNetworkStatistics() const
{
    return *NetworkStatistics_;
}

const IChunkRegistryPtr& TBootstrap::GetChunkRegistry() const
{
    return ChunkRegistry_;
}

const IBlobReaderCachePtr& TBootstrap::GetBlobReaderCache() const
{
    return BlobReaderCache_;
}

const TJobControllerPtr& TBootstrap::GetJobController() const
{
    return JobController_;
}

EJobEnvironmentType TBootstrap::GetJobEnvironmentType() const
{
    const auto& slotManagerConfig = Config_->ExecNode->SlotManager;
    return ConvertTo<EJobEnvironmentType>(slotManagerConfig->JobEnvironment->AsMap()->FindChild("type"));
}

const THashSet<ENodeFlavor>& TBootstrap::GetFlavors() const
{
    return Flavors_;
}

bool TBootstrap::IsDataNode() const
{
    return Flavors_.contains(ENodeFlavor::Data);
}

bool TBootstrap::IsExecNode() const
{
    return Flavors_.contains(ENodeFlavor::Exec);
}

bool TBootstrap::IsCellarNode() const
{
    return IsTabletNode() || IsChaosNode();
}

bool TBootstrap::IsTabletNode() const
{
    return Flavors_.contains(ENodeFlavor::Tablet);
}

bool TBootstrap::IsChaosNode() const
{
    return Flavors_.contains(ENodeFlavor::Chaos);
}

NCellarNode::IBootstrap* TBootstrap::GetCellarNodeBootstrap() const
{
    return CellarNodeBootstrap_.get();
}

NDataNode::IBootstrap* TBootstrap::GetDataNodeBootstrap() const
{
    return DataNodeBootstrap_.get();
}

NExecNode::IBootstrap* TBootstrap::GetExecNodeBootstrap() const
{
    return ExecNodeBootstrap_.get();
}

NChaosNode::IBootstrap* TBootstrap::GetChaosNodeBootstrap() const
{
    return ChaosNodeBootstrap_.get();
}

NTabletNode::IBootstrap* TBootstrap::GetTabletNodeBootstrap() const
{
    return TabletNodeBootstrap_.get();
}

void TBootstrap::DoInitialize()
{
    auto localRpcAddresses = GetLocalAddresses(Config_->Addresses, Config_->RpcPort);

    SetExplodeOnNullRowRowBufferDeserialization();

    if (!Config_->ClusterConnection->Networks) {
        Config_->ClusterConnection->Networks = GetLocalNetworks();
    }

    {
        const auto& flavors = Config_->Flavors;
        Flavors_ = THashSet<ENodeFlavor>(flavors.begin(), flavors.end());
    }

    YT_LOG_INFO("Initializing node (LocalAddresses: %v, PrimaryMasterAddresses: %v, NodeTags: %v, Flavors: %v)",
        GetValues(localRpcAddresses),
        Config_->ClusterConnection->PrimaryMaster->Addresses,
        Config_->Tags,
        Flavors_);

    // NB: Connection thread pool is required for dynamic config manager
    // initialization, so it is created before other thread pools.
    ConnectionThreadPool_ = New<TThreadPool>(
        Config_->ClusterConnection->ThreadPoolSize,
        "Connection");

    NApi::NNative::TConnectionOptions connectionOptions;
    connectionOptions.ConnectionInvoker = ConnectionThreadPool_->GetInvoker();
    connectionOptions.BlockCache = GetBlockCache();
    MasterConnection_ = NApi::NNative::CreateConnection(
        Config_->ClusterConnection,
        std::move(connectionOptions));

    MasterClient_ = MasterConnection_->CreateNativeClient(
        TClientOptions::FromUser(NSecurityClient::RootUserName));

    MemoryUsageTracker_ = New<TNodeMemoryTracker>(
        Config_->ResourceLimits->TotalMemory,
        std::vector<std::pair<EMemoryCategory, i64>>{},
        Logger,
        ClusterNodeProfiler.WithPrefix("/memory_usage"));

    MasterCacheQueue_ = New<TActionQueue>("MasterCache");
    StorageHeavyThreadPool_ = New<TThreadPool>(
        Config_->DataNode->StorageHeavyThreadCount,
        "StorageHeavy");
    StorageHeavyInvoker_ = CreatePrioritizedInvoker(StorageHeavyThreadPool_->GetInvoker());
    StorageLightThreadPool_ = New<TThreadPool>(
        Config_->DataNode->StorageLightThreadCount,
        "StorageLight");

    auto getThrottlerConfig = [&] (EDataNodeThrottlerKind kind) {
        return PatchRelativeNetworkThrottlerConfig(Config_->DataNode->Throttlers[kind]);
    };

    RawTotalInThrottler_ = CreateNamedReconfigurableThroughputThrottler(
        getThrottlerConfig(EDataNodeThrottlerKind::TotalIn),
        "TotalIn",
        ClusterNodeLogger,
        ClusterNodeProfiler.WithPrefix("/throttlers"));
    TotalInThrottler_ = IThroughputThrottlerPtr(RawTotalInThrottler_);

    RawTotalOutThrottler_ = CreateNamedReconfigurableThroughputThrottler(
        getThrottlerConfig(EDataNodeThrottlerKind::TotalOut),
        "TotalOut",
        ClusterNodeLogger,
        ClusterNodeProfiler.WithPrefix("/throttlers"));
    TotalOutThrottler_ = IThroughputThrottlerPtr(RawTotalOutThrottler_);

    RawReadRpsOutThrottler_ = CreateNamedReconfigurableThroughputThrottler(
        getThrottlerConfig(EDataNodeThrottlerKind::ReadRpsOut),
        "ReadRpsOut",
        ClusterNodeLogger,
        ClusterNodeProfiler.WithPrefix("/throttlers"));
    ReadRpsOutThrottler_ = IThroughputThrottlerPtr(RawReadRpsOutThrottler_);

    BlockCache_ = ClientBlockCache_ = CreateClientBlockCache(
        Config_->DataNode->BlockCache,
        EBlockType::UncompressedData | EBlockType::CompressedData,
        MemoryUsageTracker_->WithCategory(EMemoryCategory::BlockCache),
        DataNodeProfiler.WithPrefix("/block_cache"));

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

    LegacyMasterConnector_ = New<NDataNode::TLegacyMasterConnector>(Config_->DataNode, Config_->Tags, this);

    MasterConnector_ = NClusterNode::CreateMasterConnector(
        this,
        localRpcAddresses,
        NYT::GetLocalAddresses(Config_->Addresses, Config_->SkynetHttpPort),
        NYT::GetLocalAddresses(Config_->Addresses, Config_->MonitoringPort),
        Config_->Tags);
    MasterConnector_->SubscribePopulateAlerts(BIND(&TBootstrap::PopulateAlerts, this));
    MasterConnector_->SubscribeMasterConnected(BIND(&TBootstrap::OnMasterConnected, this));
    MasterConnector_->SubscribeMasterDisconnected(BIND(&TBootstrap::OnMasterDisconnected, this));

    DynamicConfigManager_ = New<TClusterNodeDynamicConfigManager>(this);
    DynamicConfigManager_->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));

    ChunkRegistry_ = CreateChunkRegistry(this);

    BlobReaderCache_ = CreateBlobReaderCache(this);

    ChunkMetaManager_ = CreateChunkMetaManager(this);
    VersionedChunkMetaManager_ = CreateVersionedChunkMetaManager(Config_->TabletNode->VersionedChunkMetaCache, this);

    NetworkStatistics_ = std::make_unique<TNetworkStatistics>(Config_->DataNode);

    RpcServer_->RegisterService(CreateArbitrageService(this));
    NodeResourceManager_ = New<TNodeResourceManager>(this);

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    auto localAddress = GetDefaultAddress(localRpcAddresses);

    JobController_ = New<TJobController>(Config_->ExecNode->JobController, this);

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
        MemoryUsageTracker_->WithCategory(EMemoryCategory::MasterCache),
        Logger,
        ClusterNodeProfiler.WithPrefix("/master_cache"));

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

    // NB: Data Node master connector is required for chunk cache.
    if (IsDataNode() || IsExecNode()) {
        DataNodeBootstrap_ = NDataNode::CreateBootstrap(this);
    }

    if (IsExecNode()) {
        ExecNodeBootstrap_ = NExecNode::CreateBootstrap(this);
    }

    if (IsCellarNode()) {
        CellarNodeBootstrap_ = NCellarNode::CreateBootstrap(this);
    }

    if (IsChaosNode()) {
        ChaosNodeBootstrap_ = NChaosNode::CreateBootstrap(this);
    }

    if (IsTabletNode()) {
        TabletNodeBootstrap_ = NTabletNode::CreateBootstrap(this);
    }

    RpcServer_->RegisterService(CreateAdminService(GetControlInvoker(), CoreDumper_));

    RpcServer_->Configure(Config_->RpcServer);

    if (GetJobEnvironmentType() == EJobEnvironmentType::Porto) {
        auto portoEnvironmentConfig = ConvertTo<TPortoJobEnvironmentConfigPtr>(Config_->ExecNode->SlotManager->JobEnvironment);
        auto portoExecutor = CreatePortoExecutor(
            portoEnvironmentConfig->PortoExecutor,
            "limits_tracker");

        portoExecutor->SubscribeFailed(BIND([=] (const TError& error) {
            YT_LOG_ERROR(error, "Porto executor failed");
            ExecNodeBootstrap_->GetSlotManager()->Disable(error);
        }));

        auto self = GetSelfPortoInstance(portoExecutor);
        if (Config_->InstanceLimitsUpdatePeriod) {
            auto instance = portoEnvironmentConfig->UseDaemonSubcontainer
                ? GetPortoInstance(portoExecutor, *self->GetParentName())
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

            if (Config_->ResourceLimits->NodeDedicatedCpu) {
                self->SetCpuGuarantee(*Config_->ResourceLimits->NodeDedicatedCpu);
            }

            NodeResourceManager_->SubscribeSelfMemoryGuaranteeUpdated(BIND([self] (i64 memoryGuarantee) {
                try {
                    self->SetMemoryGuarantee(memoryGuarantee);
                    YT_LOG_DEBUG("Self memory guarantee updated (MemoryGuarantee: %v)", memoryGuarantee);
                } catch (const std::exception& ex) {
                    // This probably means container limits misconfiguration on host.
                    YT_LOG_FATAL(ex, "Failed to set self memory guarantee (MemoryGuarantee: %v)", memoryGuarantee);
                }
            }));
        }
    }

    if (IsDataNode() || IsExecNode()) {
        DataNodeBootstrap_->Initialize();
    }

    if (IsExecNode()) {
        ExecNodeBootstrap_->Initialize();
    }

    if (IsCellarNode()) {
        CellarNodeBootstrap_->Initialize();
    }

    if (IsChaosNode()) {
        ChaosNodeBootstrap_->Initialize();
    }

    if (IsTabletNode()) {
        TabletNodeBootstrap_->Initialize();
    }

    // We must ensure we know actual status of job proxy binary before Run phase.
    // Otherwise we may erroneously receive some job which we fail to run due to missing
    // ytserver-job-proxy. This requires slot manager to be initialized before job controller
    // in order for the first out-of-band job proxy build info update to reach job controller
    // via signal.
    //
    // Swapping two lines below does not break anything, but introduces additional latency
    // of Config_->JobController->JobProxyBuildInfoUpdatePeriod.
    JobController_->Initialize();
}

void TBootstrap::DoRun()
{
    auto localRpcAddresses = GetLocalAddresses(Config_->Addresses, Config_->RpcPort);

    YT_LOG_INFO("Starting node (LocalAddresses: %v, PrimaryMasterAddresses: %v, NodeTags: %v)",
        GetValues(localRpcAddresses),
        Config_->ClusterConnection->PrimaryMaster->Addresses,
        Config_->Tags);

    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    DynamicConfigManager_->Start();

    NodeResourceManager_->Start();
    if (InstanceLimitsTracker_) {
        InstanceLimitsTracker_->Start();
    }

    // Force start node directory synchronizer.
    MasterConnection_->GetNodeDirectorySynchronizer()->Start();

    NMonitoring::Initialize(
        HttpServer_,
        Config_->SolomonExporter,
        &MonitoringManager_,
        &OrchidRoot_);

    SetNodeByYPath(
        OrchidRoot_,
        "/config",
        ConfigNode_);
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
        "/dynamic_config_manager",
        CreateVirtualNode(DynamicConfigManager_->GetOrchidService()
            ->Via(GetControlInvoker())));
    SetNodeByYPath(
        OrchidRoot_,
        "/object_service_cache",
        CreateVirtualNode(ObjectServiceCache_->GetOrchidService()
            ->Via(GetControlInvoker())));
    SetBuildAttributes(
        OrchidRoot_,
        "node");

    RpcServer_->RegisterService(CreateOrchidService(
        OrchidRoot_,
        GetControlInvoker()));

    YT_LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);

    YT_LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);

    // Do not start subsystems until everything is initialized.

    MasterConnector_->Initialize();
    MasterConnector_->Start();
    LegacyMasterConnector_->Start();

    DoValidateConfig();

    if (IsCellarNode()) {
        CellarNodeBootstrap_->Run();
    }

    if (IsChaosNode()) {
        ChaosNodeBootstrap_->Run();
    }

    if (IsDataNode() || IsExecNode()) {
        DataNodeBootstrap_->Run();
    }

    if (IsExecNode()) {
        ExecNodeBootstrap_->Run();
    }

    if (IsTabletNode()) {
        TabletNodeBootstrap_->Run();
    }

    RpcServer_->Start();
    HttpServer_->Start();
}

void TBootstrap::DoValidateConfig()
{
    auto unrecognized = Config_->GetUnrecognizedRecursively();
    if (unrecognized && unrecognized->GetChildCount() > 0) {
        if (Config_->EnableUnrecognizedOptionsAlert) {
            UnrecognizedOptionsAlert_ = TError(
                EErrorCode::UnrecognizedConfigOption,
                "Node config contains unrecognized options")
                << TErrorAttribute("unrecognized", unrecognized);
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
        /*isRaw*/ false,
        /*offset*/ std::nullopt,
        /*skipHeader*/ true);

    WaitFor(reader->Open())
        .ThrowOnError();

    ValidateTabletCellSnapshot(this, reader);
}

void TBootstrap::OnDynamicConfigChanged(
    const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
    const TClusterNodeDynamicConfigPtr& newConfig)
{
    ReconfigureSingletons(Config_, newConfig);

    StorageHeavyThreadPool_->Configure(
        newConfig->DataNode->StorageHeavyThreadCount.value_or(Config_->DataNode->StorageHeavyThreadCount));
    StorageLightThreadPool_->Configure(
        newConfig->DataNode->StorageLightThreadCount.value_or(Config_->DataNode->StorageLightThreadCount));

    auto getThrottlerConfig = [&] (EDataNodeThrottlerKind kind) {
        auto config = newConfig->DataNode->Throttlers[kind]
            ? newConfig->DataNode->Throttlers[kind]
            : Config_->DataNode->Throttlers[kind];
        return PatchRelativeNetworkThrottlerConfig(std::move(config));
    };
    RawTotalInThrottler_->Reconfigure(getThrottlerConfig(EDataNodeThrottlerKind::TotalIn));
    RawTotalOutThrottler_->Reconfigure(getThrottlerConfig(EDataNodeThrottlerKind::TotalOut));
    RawReadRpsOutThrottler_->Reconfigure(getThrottlerConfig(EDataNodeThrottlerKind::ReadRpsOut));

    ClientBlockCache_->Reconfigure(newConfig->DataNode->BlockCache);

    VersionedChunkMetaManager_->Reconfigure(newConfig->TabletNode->VersionedChunkMetaCache);

    ObjectServiceCache_->Reconfigure(newConfig->CachingObjectService);
    for (const auto& service : CachingObjectServices_) {
        service->Reconfigure(newConfig->CachingObjectService);
    }

    if (InstanceLimitsTracker_) {
        auto useInstanceLimitsTracker = newConfig->ResourceLimits->UseInstanceLimitsTracker;
        if (useInstanceLimitsTracker) {
            InstanceLimitsTracker_->Start();
        } else {
            InstanceLimitsTracker_->Stop();
        }
    }
}

void TBootstrap::PopulateAlerts(std::vector<TError>* alerts)
{
    PopulateAlerts_.Fire(alerts);

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

    if (!UnrecognizedOptionsAlert_.IsOK()) {
        alerts->push_back(UnrecognizedOptionsAlert_);
    }
}

void TBootstrap::OnMasterConnected(TNodeId nodeId)
{
    MasterConnected_.Fire(nodeId);

    for (const auto& cachingObjectService : CachingObjectServices_) {
        RpcServer_->RegisterService(cachingObjectService);
    }
}

void TBootstrap::OnMasterDisconnected()
{
    MasterDisconnected_.Fire();

    for (const auto& cachingObjectService : CachingObjectServices_) {
        RpcServer_->UnregisterService(cachingObjectService);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TClusterNodeConfigPtr config, NYTree::INodePtr configNode)
{
    return std::make_unique<TBootstrap>(std::move(config), std::move(configNode));
}

////////////////////////////////////////////////////////////////////////////////

TBootstrapForward::TBootstrapForward(IBootstrapBase* bootstrap)
    : Bootstrap_(bootstrap)
{
    Bootstrap_->SubscribeMasterConnected(
        BIND([this] (TNodeId nodeId) {
            MasterConnected_.Fire(nodeId);
        }));
    Bootstrap_->SubscribeMasterDisconnected(
        BIND([this] {
            MasterDisconnected_.Fire();
        }));
    Bootstrap_->SubscribePopulateAlerts(
        BIND([this] (std::vector<TError>* alerts) {
            PopulateAlerts_.Fire(alerts);
        }));
}

const TNodeMemoryTrackerPtr& TBootstrapForward::GetMemoryUsageTracker() const
{
    return Bootstrap_->GetMemoryUsageTracker();
}

const TNodeResourceManagerPtr& TBootstrapForward::GetNodeResourceManager() const
{
    return Bootstrap_->GetNodeResourceManager();
}

const IThroughputThrottlerPtr& TBootstrapForward::GetTotalInThrottler() const
{
    return Bootstrap_->GetTotalInThrottler();
}

const IThroughputThrottlerPtr& TBootstrapForward::GetTotalOutThrottler() const
{
    return Bootstrap_->GetTotalOutThrottler();
}

const IThroughputThrottlerPtr& TBootstrapForward::GetReadRpsOutThrottler() const
{
    return Bootstrap_->GetReadRpsOutThrottler();
}

const TClusterNodeConfigPtr& TBootstrapForward::GetConfig() const
{
    return Bootstrap_->GetConfig();
}

const TClusterNodeDynamicConfigManagerPtr& TBootstrapForward::GetDynamicConfigManager() const
{
    return Bootstrap_->GetDynamicConfigManager();
}

const IInvokerPtr& TBootstrapForward::GetControlInvoker() const
{
    return Bootstrap_->GetControlInvoker();
}

const IInvokerPtr& TBootstrapForward::GetJobInvoker() const
{
    return Bootstrap_->GetJobInvoker();
}

const IInvokerPtr& TBootstrapForward::GetMasterConnectionInvoker() const
{
    return Bootstrap_->GetMasterConnectionInvoker();
}

const IInvokerPtr& TBootstrapForward::GetStorageLightInvoker() const
{
    return Bootstrap_->GetStorageLightInvoker();
}

const IPrioritizedInvokerPtr& TBootstrapForward::GetStorageHeavyInvoker() const
{
    return Bootstrap_->GetStorageHeavyInvoker();
}

const NNative::IClientPtr& TBootstrapForward::GetMasterClient() const
{
    return Bootstrap_->GetMasterClient();
}

const NNative::IConnectionPtr& TBootstrapForward::GetMasterConnection() const
{
    return Bootstrap_->GetMasterConnection();
}

IChannelPtr TBootstrapForward::GetMasterChannel(TCellTag cellTag)
{
    return Bootstrap_->GetMasterChannel(cellTag);
}

TNodeDescriptor TBootstrapForward::GetLocalDescriptor() const
{
    return Bootstrap_->GetLocalDescriptor();
}

TCellId TBootstrapForward::GetCellId() const
{
    return Bootstrap_->GetCellId();
}

TCellId TBootstrapForward::GetCellId(TCellTag cellTag) const
{
    return Bootstrap_->GetCellId(cellTag);
}

const TCellTagList& TBootstrapForward::GetMasterCellTags() const
{
    return Bootstrap_->GetMasterCellTags();
}

std::vector<TString> TBootstrapForward::GetMasterAddressesOrThrow(TCellTag cellTag) const
{
    return Bootstrap_->GetMasterAddressesOrThrow(cellTag);
}

const TLegacyMasterConnectorPtr& TBootstrapForward::GetLegacyMasterConnector() const
{
    return Bootstrap_->GetLegacyMasterConnector();
}

bool TBootstrapForward::UseNewHeartbeats() const
{
    return Bootstrap_->UseNewHeartbeats();
}

void TBootstrapForward::ResetAndRegisterAtMaster()
{
    return Bootstrap_->ResetAndRegisterAtMaster();
}

bool TBootstrapForward::IsConnected() const
{
    return Bootstrap_->IsConnected();
}

TNodeId TBootstrapForward::GetNodeId() const
{
    return Bootstrap_->GetNodeId();
}

TString TBootstrapForward::GetLocalHostName() const
{
    return Bootstrap_->GetLocalHostName();
}

TMasterEpoch TBootstrapForward::GetMasterEpoch() const
{
    return Bootstrap_->GetMasterEpoch();
}

const TNodeDirectoryPtr& TBootstrapForward::GetNodeDirectory() const
{
    return Bootstrap_->GetNodeDirectory();
}

TNetworkPreferenceList TBootstrapForward::GetLocalNetworks() const
{
    return Bootstrap_->GetLocalNetworks();
}

std::optional<TString> TBootstrapForward::GetDefaultNetworkName() const
{
    return Bootstrap_->GetDefaultNetworkName();
}

TString TBootstrapForward::GetDefaultLocalAddressOrThrow() const
{
    return Bootstrap_->GetDefaultLocalAddressOrThrow();
}

const NHttp::IServerPtr& TBootstrapForward::GetHttpServer() const
{
    return Bootstrap_->GetHttpServer();
}

const NRpc::IServerPtr& TBootstrapForward::GetRpcServer() const
{
    return Bootstrap_->GetRpcServer();
}

const IBlockCachePtr& TBootstrapForward::GetBlockCache() const
{
    return Bootstrap_->GetBlockCache();
}

const IClientBlockCachePtr& TBootstrapForward::GetClientBlockCache() const
{
    return Bootstrap_->GetClientBlockCache();
}

const IChunkMetaManagerPtr& TBootstrapForward::GetChunkMetaManager() const
{
    return Bootstrap_->GetChunkMetaManager();
}

const IVersionedChunkMetaManagerPtr& TBootstrapForward::GetVersionedChunkMetaManager() const
{
    return Bootstrap_->GetVersionedChunkMetaManager();
}

const IMapNodePtr& TBootstrapForward::GetOrchidRoot() const
{
    return Bootstrap_->GetOrchidRoot();
}

bool TBootstrapForward::IsReadOnly() const
{
    return Bootstrap_->IsReadOnly();
}

bool TBootstrapForward::Decommissioned() const
{
    return Bootstrap_->Decommissioned();
}

TNetworkStatistics& TBootstrapForward::GetNetworkStatistics() const
{
    return Bootstrap_->GetNetworkStatistics();
}

const IChunkRegistryPtr& TBootstrapForward::GetChunkRegistry() const
{
    return Bootstrap_->GetChunkRegistry();
}

const IBlobReaderCachePtr& TBootstrapForward::GetBlobReaderCache() const
{
    return Bootstrap_->GetBlobReaderCache();
}

const TJobControllerPtr& TBootstrapForward::GetJobController() const
{
    return Bootstrap_->GetJobController();
}

EJobEnvironmentType TBootstrapForward::GetJobEnvironmentType() const
{
    return Bootstrap_->GetJobEnvironmentType();
}

const THashSet<ENodeFlavor>& TBootstrapForward::GetFlavors() const
{
    return Bootstrap_->GetFlavors();
}

bool TBootstrapForward::IsDataNode() const
{
    return Bootstrap_->IsDataNode();
}

bool TBootstrapForward::IsExecNode() const
{
    return Bootstrap_->IsExecNode();
}

bool TBootstrapForward::IsCellarNode() const
{
    return Bootstrap_->IsCellarNode();
}

bool TBootstrapForward::IsTabletNode() const
{
    return Bootstrap_->IsTabletNode();
}

bool TBootstrapForward::IsChaosNode() const
{
    return Bootstrap_->IsChaosNode();
}

NCellarNode::IBootstrap* TBootstrapForward::GetCellarNodeBootstrap() const
{
    return Bootstrap_->GetCellarNodeBootstrap();
}

NDataNode::IBootstrap* TBootstrapForward::GetDataNodeBootstrap() const
{
    return Bootstrap_->GetDataNodeBootstrap();
}

NExecNode::IBootstrap* TBootstrapForward::GetExecNodeBootstrap() const
{
    return Bootstrap_->GetExecNodeBootstrap();
}

NChaosNode::IBootstrap* TBootstrapForward::GetChaosNodeBootstrap() const
{
    return Bootstrap_->GetChaosNodeBootstrap();
}

NTabletNode::IBootstrap* TBootstrapForward::GetTabletNodeBootstrap() const
{
    return Bootstrap_->GetTabletNodeBootstrap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
