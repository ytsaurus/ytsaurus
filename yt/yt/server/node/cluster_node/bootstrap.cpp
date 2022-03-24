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
#include <yt/yt/server/node/data_node/chunk_reader_sweeper.h>
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

#include <yt/yt/server/lib/io/config.h>
#include <yt/yt/server/lib/io/io_tracker.h>

#ifdef __linux__
#include <yt/yt/server/lib/containers/instance.h>
#include <yt/yt/server/lib/containers/instance_limits_tracker.h>
#include <yt/yt/server/lib/containers/porto_executor.h>
#endif

#include <yt/yt/server/lib/core_dump/core_dumper.h>

#include <yt/yt/server/lib/hydra_common/snapshot.h>
#include <yt/yt/server/lib/hydra_common/file_snapshot_store.h>

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
#include <yt/yt/core/concurrency/fair_throttler.h>

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
using namespace NIO;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("Bootstrap");

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(NNodeTrackerClient::TNodeId nodeId), MasterConnected);
    DEFINE_SIGNAL_OVERRIDE(void(), MasterDisconnected);
    DEFINE_SIGNAL_OVERRIDE(void(std::vector<TError>* alerts), PopulateAlerts);

public:
    TBootstrap(TClusterNodeConfigPtr config, INodePtr configNode)
        : Config_(std::move(config))
        , ConfigNode_(std::move(configNode))
    { }

    // IBootstrap implementation.
    void Initialize() override
    {
        ControlActionQueue_ = New<TActionQueue>("Control");
        JobActionQueue_ = New<TActionQueue>("Job");

        BIND(&TBootstrap::DoInitialize, this)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

    void Run() override
    {
        BIND(&TBootstrap::DoRun, this)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();

        Sleep(TDuration::Max());
    }

    void ValidateSnapshot(const TString& fileName) override
    {
        BIND(&TBootstrap::DoValidateSnapshot, this, fileName)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

    const IMasterConnectorPtr& GetMasterConnector() const override
    {
        return MasterConnector_;
    }

    TRelativeThroughputThrottlerConfigPtr PatchRelativeNetworkThrottlerConfig(
        const TRelativeThroughputThrottlerConfigPtr& config) const override
    {
        // NB: Absolute value limit suppresses relative one.
        if (config->Limit || !config->RelativeLimit) {
            return config;
        }

        auto patchedConfig = CloneYsonSerializable(config);
        patchedConfig->Limit = *config->RelativeLimit * Config_->NetworkBandwidth;

        return patchedConfig;
    }

    void SetDecommissioned(bool decommissioned) override
    {
        Decommissioned_ = decommissioned;
    }

    // IBootstrapBase implementation.
    const TNodeMemoryTrackerPtr& GetMemoryUsageTracker() const override
    {
        return MemoryUsageTracker_;
    }

    const TNodeResourceManagerPtr& GetNodeResourceManager() const override
    {
        return NodeResourceManager_;
    }

    const NConcurrency::IThroughputThrottlerPtr& GetDefaultInThrottler() const override
    {
        if (Config_->EnableFairThrottler) {
            return DefaultInThrottler_;
        } else {
            return LegacyTotalInThrottler_;
        }
    }

    const NConcurrency::IThroughputThrottlerPtr& GetDefaultOutThrottler() const override
    {
        if (Config_->EnableFairThrottler) {
            return DefaultOutThrottler_;
        } else {
            return LegacyTotalOutThrottler_;
        }
    }

    const NConcurrency::IThroughputThrottlerPtr& GetReadRpsOutThrottler() const override
    {
        return ReadRpsOutThrottler_;
    }

    const NConcurrency::IThroughputThrottlerPtr& GetAnnounceChunkReplicaRpsOutThrottler() const override
    {
        return AnnounceChunkReplicaRpsOutThrottler_;
    }

    const TBufferedProducerPtr& GetBufferedProducer() const override
    {
        return BufferedProducer_;
    }

    const TClusterNodeConfigPtr& GetConfig() const override
    {
        return Config_;
    }

    const NClusterNode::TClusterNodeDynamicConfigManagerPtr& GetDynamicConfigManager() const override
    {
        return DynamicConfigManager_;
    }

    const IInvokerPtr& GetControlInvoker() const override
    {
        return ControlActionQueue_->GetInvoker();
    }

    const IInvokerPtr& GetJobInvoker() const override
    {
        return JobActionQueue_->GetInvoker();
    }

    const IInvokerPtr& GetMasterConnectionInvoker() const override
    {
        return MasterConnector_->GetMasterConnectionInvoker();
    }

    const IInvokerPtr& GetStorageLightInvoker() const override
    {
        return StorageLightThreadPool_->GetInvoker();
    }

    const IPrioritizedInvokerPtr& GetStorageHeavyInvoker() const override
    {
        return StorageHeavyInvoker_;
    }

    const NApi::NNative::IClientPtr& GetMasterClient() const override
    {
        return MasterClient_;
    }

    const NApi::NNative::IConnectionPtr& GetMasterConnection() const override
    {
        return MasterConnection_;
    }

    IChannelPtr GetMasterChannel(TCellTag cellTag) override
    {
        return MasterConnector_->GetMasterChannel(cellTag);
    }

    TNodeDescriptor GetLocalDescriptor() const override
    {
        return MasterConnector_->GetLocalDescriptor();
    }

    TCellId GetCellId() const override
    {
        return Config_->ClusterConnection->PrimaryMaster->CellId;
    }

    TCellId GetCellId(TCellTag cellTag) const override
    {
        return cellTag == PrimaryMasterCellTagSentinel
            ? GetCellId()
            : ReplaceCellTagInId(GetCellId(), cellTag);
    }

    const TCellTagList& GetMasterCellTags() const override
    {
        return MasterConnector_->GetMasterCellTags();
    }

    std::vector<TString> GetMasterAddressesOrThrow(TCellTag cellTag) const override
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

    const TLegacyMasterConnectorPtr& GetLegacyMasterConnector() const override
    {
        return LegacyMasterConnector_;
    }

    bool UseNewHeartbeats() const override
    {
        return MasterConnector_->UseNewHeartbeats();
    }

    void ResetAndRegisterAtMaster() override
    {
        return MasterConnector_->ResetAndRegisterAtMaster();
    }

    bool IsConnected() const override
    {
        return MasterConnector_->IsConnected();
    }

    TNodeId GetNodeId() const override
    {
        return MasterConnector_->GetNodeId();
    }

    TString GetLocalHostName() const override
    {
        return MasterConnector_->GetLocalHostName();
    }

    TMasterEpoch GetMasterEpoch() const override
    {
        return MasterConnector_->GetEpoch();
    }

    const TNodeDirectoryPtr& GetNodeDirectory() const override
    {
        return MasterConnection_->GetNodeDirectory();
    }

    TNetworkPreferenceList GetLocalNetworks() const override
    {
        return Config_->Addresses.empty()
            ? DefaultNetworkPreferences
            : GetIths<0>(Config_->Addresses);
    }

    std::optional<TString> GetDefaultNetworkName() const override
    {
        return Config_->BusServer->DefaultNetwork;
    }

    TString GetDefaultLocalAddressOrThrow() const override
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

    const NHttp::IServerPtr& GetHttpServer() const override
    {
        return HttpServer_;
    }

    const NRpc::IServerPtr& GetRpcServer() const override
    {
        return RpcServer_;
    }

    const IBlockCachePtr& GetBlockCache() const override
    {
        return BlockCache_;
    }

    const IClientBlockCachePtr& GetClientBlockCache() const override
    {
        return ClientBlockCache_;
    }

    const IChunkMetaManagerPtr& GetChunkMetaManager() const override
    {
        return ChunkMetaManager_;
    }

    const NDataNode::TChunkReaderSweeperPtr& GetChunkReaderSweeper() const override
    {
        return ChunkReaderSweeper_;
    }

    const IVersionedChunkMetaManagerPtr& GetVersionedChunkMetaManager() const override
    {
        return VersionedChunkMetaManager_;
    }

    const NYTree::IMapNodePtr& GetOrchidRoot() const override
    {
        return OrchidRoot_;
    }

    bool IsReadOnly() const override
    {
        // TOOD(gritukan): Make node without dynamic config read-only after YT-12933.
        return false;
    }

    bool IsDecommissioned() const override
    {
        return Decommissioned_;
    }

    NDataNode::TNetworkStatistics& GetNetworkStatistics() const override
    {
        return *NetworkStatistics_;
    }

    const IChunkRegistryPtr& GetChunkRegistry() const override
    {
        return ChunkRegistry_;
    }

    const IBlobReaderCachePtr& GetBlobReaderCache() const override
    {
        return BlobReaderCache_;
    }

    const TJobControllerPtr& GetJobController() const override
    {
        return JobController_;
    }

    const IIOTrackerPtr& GetIOTracker() const override
    {
        return IOTracker_;
    }

    EJobEnvironmentType GetJobEnvironmentType() const override
    {
        const auto& slotManagerConfig = Config_->ExecNode->SlotManager;
        return ConvertTo<EJobEnvironmentType>(slotManagerConfig->JobEnvironment->AsMap()->FindChild("type"));
    }

    const THashSet<ENodeFlavor>& GetFlavors() const override
    {
        return Flavors_;
    }

    bool IsDataNode() const override
    {
        return Flavors_.contains(ENodeFlavor::Data);
    }

    bool IsExecNode() const override
    {
        return Flavors_.contains(ENodeFlavor::Exec);
    }

    bool IsCellarNode() const override
    {
        return IsTabletNode() || IsChaosNode();
    }

    bool IsTabletNode() const override
    {
        return Flavors_.contains(ENodeFlavor::Tablet);
    }

    bool IsChaosNode() const override
    {
        return Flavors_.contains(ENodeFlavor::Chaos);
    }

    NCellarNode::IBootstrap* GetCellarNodeBootstrap() const override
    {
        return CellarNodeBootstrap_.get();
    }

    NDataNode::IBootstrap* GetDataNodeBootstrap() const override
    {
        return DataNodeBootstrap_.get();
    }

    NExecNode::IBootstrap* GetExecNodeBootstrap() const override
    {
        return ExecNodeBootstrap_.get();
    }

    NChaosNode::IBootstrap* GetChaosNodeBootstrap() const override
    {
        return ChaosNodeBootstrap_.get();
    }

    NTabletNode::IBootstrap* GetTabletNodeBootstrap() const override
    {
        return TabletNodeBootstrap_.get();
    }

private:
    const TClusterNodeConfigPtr Config_;
    const INodePtr ConfigNode_;

    TActionQueuePtr ControlActionQueue_;
    TActionQueuePtr JobActionQueue_;
    TThreadPoolPtr ConnectionThreadPool_;
    TThreadPoolPtr StorageLightThreadPool_;
    TThreadPoolPtr StorageHeavyThreadPool_;
    IPrioritizedInvokerPtr StorageHeavyInvoker_;
    TActionQueuePtr MasterCacheQueue_;

    ICoreDumperPtr CoreDumper_;

    TMonitoringManagerPtr MonitoringManager_;

    NYT::NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    IMapNodePtr OrchidRoot_;

    TNodeMemoryTrackerPtr MemoryUsageTracker_;
    TNodeResourceManagerPtr NodeResourceManager_;
    TBufferedProducerPtr BufferedProducer_;

    IReconfigurableThroughputThrottlerPtr LegacyRawTotalInThrottler_;
    IThroughputThrottlerPtr LegacyTotalInThrottler_;

    TFairThrottlerPtr InThrottler_;
    IThroughputThrottlerPtr DefaultInThrottler_;
    THashSet<TString> EnabledInThrottlers_;

    IReconfigurableThroughputThrottlerPtr LegacyRawTotalOutThrottler_;
    IThroughputThrottlerPtr LegacyTotalOutThrottler_;

    TFairThrottlerPtr OutThrottler_;
    IThroughputThrottlerPtr DefaultOutThrottler_;
    THashSet<TString> EnabledOutThrottlers_;

    IReconfigurableThroughputThrottlerPtr RawReadRpsOutThrottler_;
    IThroughputThrottlerPtr ReadRpsOutThrottler_;

    IReconfigurableThroughputThrottlerPtr RawAnnounceChunkReplicaRpsOutThrottler_;
    IThroughputThrottlerPtr AnnounceChunkReplicaRpsOutThrottler_;

#ifdef __linux__
    NContainers::TInstanceLimitsTrackerPtr InstanceLimitsTracker_;
#endif

    TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;

    NApi::NNative::IClientPtr MasterClient_;
    NApi::NNative::IConnectionPtr MasterConnection_;

    TJobControllerPtr JobController_;

    IMasterConnectorPtr MasterConnector_;
    TLegacyMasterConnectorPtr LegacyMasterConnector_;

    IBlockCachePtr BlockCache_;
    IClientBlockCachePtr ClientBlockCache_;

    std::unique_ptr<TNetworkStatistics> NetworkStatistics_;

    IChunkMetaManagerPtr ChunkMetaManager_;
    IVersionedChunkMetaManagerPtr VersionedChunkMetaManager_;

    IChunkRegistryPtr ChunkRegistry_;
    IBlobReaderCachePtr BlobReaderCache_;

    TChunkReaderSweeperPtr ChunkReaderSweeper_;

    TError UnrecognizedOptionsAlert_;

    TObjectServiceCachePtr ObjectServiceCache_;
    std::vector<ICachingObjectServicePtr> CachingObjectServices_;

    IIOTrackerPtr IOTracker_;

    THashSet<ENodeFlavor> Flavors_;

    std::unique_ptr<NCellarNode::IBootstrap> CellarNodeBootstrap_;
    std::unique_ptr<NChaosNode::IBootstrap> ChaosNodeBootstrap_;
    std::unique_ptr<NExecNode::IBootstrap> ExecNodeBootstrap_;
    std::unique_ptr<NDataNode::IBootstrap> DataNodeBootstrap_;
    std::unique_ptr<NTabletNode::IBootstrap> TabletNodeBootstrap_;

    bool Decommissioned_ = false;

    void DoInitialize()
    {
        auto localRpcAddresses = GetLocalAddresses(Config_->Addresses, Config_->RpcPort);

        if (!Config_->ClusterConnection->Networks) {
            Config_->ClusterConnection->Networks = GetLocalNetworks();
        }

        {
            const auto& flavors = Config_->Flavors;
            Flavors_ = THashSet<ENodeFlavor>(flavors.begin(), flavors.end());
        }

        YT_LOG_INFO("Initializing cluster node (LocalAddresses: %v, PrimaryMasterAddresses: %v, NodeTags: %v, Flavors: %v)",
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

        BufferedProducer_ = New<TBufferedProducer>();
        ClusterNodeProfiler.WithProducerRemoveSupport().AddProducer("", BufferedProducer_);

        MasterCacheQueue_ = New<TActionQueue>("MasterCache");
        StorageHeavyThreadPool_ = New<TThreadPool>(
            Config_->DataNode->StorageHeavyThreadCount,
            "StorageHeavy");
        StorageHeavyInvoker_ = CreatePrioritizedInvoker(StorageHeavyThreadPool_->GetInvoker());
        StorageLightThreadPool_ = New<TThreadPool>(
            Config_->DataNode->StorageLightThreadCount,
            "StorageLight");

        if (Config_->EnableFairThrottler) {
            auto fairThrottlerConfig = New<TFairThrottlerConfig>();
            fairThrottlerConfig->TotalLimit = Config_->NetworkBandwidth;

            InThrottler_ = New<TFairThrottler>(
                fairThrottlerConfig,
                ClusterNodeLogger.WithTag("Direction: %v", "In"),
                ClusterNodeProfiler.WithPrefix("/in_throttler"));
            DefaultInThrottler_ = GetInThrottler("default");

            OutThrottler_ = New<TFairThrottler>(
                fairThrottlerConfig,
                ClusterNodeLogger.WithTag("Direction: %v", "Out"),
                ClusterNodeProfiler.WithPrefix("/out_throttler"));
            DefaultOutThrottler_ = GetOutThrottler("default");
        } else {
            auto getThrottlerConfig = [&] (EDataNodeThrottlerKind kind) {
                return PatchRelativeNetworkThrottlerConfig(Config_->DataNode->Throttlers[kind]);
            };

            LegacyRawTotalInThrottler_ = CreateNamedReconfigurableThroughputThrottler(
                getThrottlerConfig(EDataNodeThrottlerKind::TotalIn),
                "TotalIn",
                ClusterNodeLogger,
                ClusterNodeProfiler.WithPrefix("/throttlers"));
            LegacyTotalInThrottler_ = IThroughputThrottlerPtr(LegacyRawTotalInThrottler_);

            LegacyRawTotalOutThrottler_ = CreateNamedReconfigurableThroughputThrottler(
                getThrottlerConfig(EDataNodeThrottlerKind::TotalOut),
                "TotalOut",
                ClusterNodeLogger,
                ClusterNodeProfiler.WithPrefix("/throttlers"));
            LegacyTotalOutThrottler_ = IThroughputThrottlerPtr(LegacyRawTotalOutThrottler_);
        }

        RawReadRpsOutThrottler_ = CreateNamedReconfigurableThroughputThrottler(
            Config_->DataNode->ReadRpsOutThrottler,
            "ReadRpsOut",
            ClusterNodeLogger,
            ClusterNodeProfiler.WithPrefix("/out_read_rps_throttler"));
        ReadRpsOutThrottler_ = IThroughputThrottlerPtr(RawReadRpsOutThrottler_);

        RawAnnounceChunkReplicaRpsOutThrottler_ = CreateNamedReconfigurableThroughputThrottler(
            Config_->DataNode->AnnounceChunkReplicaRpsOutThrottler,
            "AnnounceChunkReplicaRpsOut",
            ClusterNodeLogger,
            ClusterNodeProfiler.WithPrefix("/out_announce_chunk_replica_rps_throttler"));
        AnnounceChunkReplicaRpsOutThrottler_ = IThroughputThrottlerPtr(RawAnnounceChunkReplicaRpsOutThrottler_);

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

        IOTracker_ = CreateIOTracker(DynamicConfigManager_->GetConfig()->IOTracker);

        ChunkRegistry_ = CreateChunkRegistry(this);
        ChunkReaderSweeper_ = New<TChunkReaderSweeper>(
            GetDynamicConfigManager(),
            GetStorageHeavyInvoker());

        ChunkMetaManager_ = CreateChunkMetaManager(
            Config_->DataNode,
            GetDynamicConfigManager(),
            GetMemoryUsageTracker());

        BlobReaderCache_ = CreateBlobReaderCache(
            GetConfig()->DataNode,
            GetDynamicConfigManager(),
            ChunkMetaManager_);

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

    #ifdef __linux__
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
                        YT_LOG_ALERT(ex, "Failed to set self memory guarantee (MemoryGuarantee: %v)", memoryGuarantee);
                    }
                }));
            }
        }
    #endif

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

    void DoRun()
    {
        auto localRpcAddresses = GetLocalAddresses(Config_->Addresses, Config_->RpcPort);

        YT_LOG_INFO("Starting node (LocalAddresses: %v, PrimaryMasterAddresses: %v, NodeTags: %v)",
            GetValues(localRpcAddresses),
            Config_->ClusterConnection->PrimaryMaster->Addresses,
            Config_->Tags);

        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        DynamicConfigManager_->Start();

        NodeResourceManager_->Start();

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
            CreateVirtualNode(ConfigNode_));
        SetNodeByYPath(
            OrchidRoot_,
            "/job_controller",
            CreateVirtualNode(JobController_->GetOrchidService()));
        SetNodeByYPath(
            OrchidRoot_,
            "/cluster_connection",
            CreateVirtualNode(MasterConnection_->GetOrchidService()));
        SetNodeByYPath(
            OrchidRoot_,
            "/dynamic_config_manager",
            CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));
        SetNodeByYPath(
            OrchidRoot_,
            "/object_service_cache",
            CreateVirtualNode(ObjectServiceCache_->GetOrchidService()));
        SetBuildAttributes(
            OrchidRoot_,
            "node");

#ifdef __linux__
        if (InstanceLimitsTracker_) {
            InstanceLimitsTracker_->Start();
            SetNodeByYPath(
                OrchidRoot_,
                "/instance_limits_tracker",
                CreateVirtualNode(InstanceLimitsTracker_->GetOrchidService()));
        }
#endif

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

    void DoValidateConfig()
    {
        auto unrecognized = Config_->GetRecursiveUnrecognized();
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

    void DoValidateSnapshot(const TString& fileName)
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

    NConcurrency::IThroughputThrottlerPtr GetInThrottler(const TString& bucket) override
    {
        EnabledInThrottlers_.insert(bucket);
        return InThrottler_->CreateBucketThrottler(bucket, Config_->InThrottlers[bucket]);
    }

    NConcurrency::IThroughputThrottlerPtr GetOutThrottler(const TString& bucket) override
    {
        EnabledOutThrottlers_.insert(bucket);
        return OutThrottler_->CreateBucketThrottler(bucket, Config_->OutThrottlers[bucket]);
    }

    void ReconfigureThrottlers(const TClusterNodeDynamicConfigPtr& newConfig)
    {
        auto throttlerConfig = New<TFairThrottlerConfig>();
        throttlerConfig->TotalLimit = Config_->NetworkBandwidth;

        THashMap<TString, TFairThrottlerBucketConfigPtr> inBucketsConfig;
        for (const auto& bucket : EnabledInThrottlers_) {
            inBucketsConfig[bucket] = Config_->InThrottlers[bucket];
            if (newConfig->InThrottlers[bucket]) {
                inBucketsConfig[bucket] = newConfig->InThrottlers[bucket];
            }
        }
        InThrottler_->Reconfigure(throttlerConfig, inBucketsConfig);

        THashMap<TString, TFairThrottlerBucketConfigPtr> outBucketsConfig;
        for (const auto& bucket : EnabledOutThrottlers_) {
            outBucketsConfig[bucket] = Config_->OutThrottlers[bucket];
            if (newConfig->OutThrottlers[bucket]) {
                outBucketsConfig[bucket] = newConfig->OutThrottlers[bucket];
            }
        }
        OutThrottler_->Reconfigure(throttlerConfig, outBucketsConfig);
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        ReconfigureSingletons(Config_, newConfig);

        StorageHeavyThreadPool_->Configure(
            newConfig->DataNode->StorageHeavyThreadCount.value_or(Config_->DataNode->StorageHeavyThreadCount));
        StorageLightThreadPool_->Configure(
            newConfig->DataNode->StorageLightThreadCount.value_or(Config_->DataNode->StorageLightThreadCount));

        if (Config_->EnableFairThrottler) {
            ReconfigureThrottlers(newConfig);
        } else {
            auto getThrottlerConfig = [&] (EDataNodeThrottlerKind kind) {
                auto config = newConfig->DataNode->Throttlers[kind]
                    ? newConfig->DataNode->Throttlers[kind]
                    : Config_->DataNode->Throttlers[kind];
                return PatchRelativeNetworkThrottlerConfig(std::move(config));
            };
            LegacyRawTotalInThrottler_->Reconfigure(getThrottlerConfig(EDataNodeThrottlerKind::TotalIn));
            LegacyRawTotalOutThrottler_->Reconfigure(getThrottlerConfig(EDataNodeThrottlerKind::TotalOut));
        }

        RawReadRpsOutThrottler_->Reconfigure(newConfig->DataNode->ReadRpsOutThrottler
                ? newConfig->DataNode->ReadRpsOutThrottler
                : Config_->DataNode->ReadRpsOutThrottler);
        RawAnnounceChunkReplicaRpsOutThrottler_->Reconfigure(newConfig->DataNode->AnnounceChunkReplicaRpsOutThrottler
                ? newConfig->DataNode->AnnounceChunkReplicaRpsOutThrottler
                : Config_->DataNode->AnnounceChunkReplicaRpsOutThrottler);

        ClientBlockCache_->Reconfigure(newConfig->DataNode->BlockCache);

        VersionedChunkMetaManager_->Reconfigure(newConfig->TabletNode->VersionedChunkMetaCache);

        ObjectServiceCache_->Reconfigure(newConfig->CachingObjectService);
        for (const auto& service : CachingObjectServices_) {
            service->Reconfigure(newConfig->CachingObjectService);
        }

        IOTracker_->SetConfig(newConfig->IOTracker);

    #ifdef __linux__
        if (InstanceLimitsTracker_) {
            auto useInstanceLimitsTracker = newConfig->ResourceLimits->UseInstanceLimitsTracker;
            if (useInstanceLimitsTracker) {
                InstanceLimitsTracker_->Start();
            } else {
                InstanceLimitsTracker_->Stop();
            }
        }
    #endif
    }

    void PopulateAlerts(std::vector<TError>* alerts)
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

    void OnMasterConnected(TNodeId nodeId)
    {
        MasterConnected_.Fire(nodeId);

        for (const auto& cachingObjectService : CachingObjectServices_) {
            RpcServer_->RegisterService(cachingObjectService);
        }
    }

    void OnMasterDisconnected()
    {
        MasterDisconnected_.Fire();

        for (const auto& cachingObjectService : CachingObjectServices_) {
            RpcServer_->UnregisterService(cachingObjectService);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TClusterNodeConfigPtr config, NYTree::INodePtr configNode)
{
    return std::make_unique<TBootstrap>(std::move(config), std::move(configNode));
}

////////////////////////////////////////////////////////////////////////////////

TBootstrapBase::TBootstrapBase(IBootstrapBase* bootstrap)
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

const TNodeMemoryTrackerPtr& TBootstrapBase::GetMemoryUsageTracker() const
{
    return Bootstrap_->GetMemoryUsageTracker();
}

const TNodeResourceManagerPtr& TBootstrapBase::GetNodeResourceManager() const
{
    return Bootstrap_->GetNodeResourceManager();
}

const IThroughputThrottlerPtr& TBootstrapBase::GetDefaultInThrottler() const
{
    return Bootstrap_->GetDefaultInThrottler();
}

const IThroughputThrottlerPtr& TBootstrapBase::GetDefaultOutThrottler() const
{
    return Bootstrap_->GetDefaultOutThrottler();
}

const IThroughputThrottlerPtr& TBootstrapBase::GetReadRpsOutThrottler() const
{
    return Bootstrap_->GetReadRpsOutThrottler();
}

const IThroughputThrottlerPtr& TBootstrapBase::GetAnnounceChunkReplicaRpsOutThrottler() const
{
    return Bootstrap_->GetAnnounceChunkReplicaRpsOutThrottler();
}

const TBufferedProducerPtr& TBootstrapBase::GetBufferedProducer() const
{
    return Bootstrap_->GetBufferedProducer();
}

const TClusterNodeConfigPtr& TBootstrapBase::GetConfig() const
{
    return Bootstrap_->GetConfig();
}

const TClusterNodeDynamicConfigManagerPtr& TBootstrapBase::GetDynamicConfigManager() const
{
    return Bootstrap_->GetDynamicConfigManager();
}

const IInvokerPtr& TBootstrapBase::GetControlInvoker() const
{
    return Bootstrap_->GetControlInvoker();
}

const IInvokerPtr& TBootstrapBase::GetJobInvoker() const
{
    return Bootstrap_->GetJobInvoker();
}

const IInvokerPtr& TBootstrapBase::GetMasterConnectionInvoker() const
{
    return Bootstrap_->GetMasterConnectionInvoker();
}

const IInvokerPtr& TBootstrapBase::GetStorageLightInvoker() const
{
    return Bootstrap_->GetStorageLightInvoker();
}

const IPrioritizedInvokerPtr& TBootstrapBase::GetStorageHeavyInvoker() const
{
    return Bootstrap_->GetStorageHeavyInvoker();
}

const NNative::IClientPtr& TBootstrapBase::GetMasterClient() const
{
    return Bootstrap_->GetMasterClient();
}

const NNative::IConnectionPtr& TBootstrapBase::GetMasterConnection() const
{
    return Bootstrap_->GetMasterConnection();
}

IChannelPtr TBootstrapBase::GetMasterChannel(TCellTag cellTag)
{
    return Bootstrap_->GetMasterChannel(cellTag);
}

TNodeDescriptor TBootstrapBase::GetLocalDescriptor() const
{
    return Bootstrap_->GetLocalDescriptor();
}

TCellId TBootstrapBase::GetCellId() const
{
    return Bootstrap_->GetCellId();
}

TCellId TBootstrapBase::GetCellId(TCellTag cellTag) const
{
    return Bootstrap_->GetCellId(cellTag);
}

const TCellTagList& TBootstrapBase::GetMasterCellTags() const
{
    return Bootstrap_->GetMasterCellTags();
}

std::vector<TString> TBootstrapBase::GetMasterAddressesOrThrow(TCellTag cellTag) const
{
    return Bootstrap_->GetMasterAddressesOrThrow(cellTag);
}

const TLegacyMasterConnectorPtr& TBootstrapBase::GetLegacyMasterConnector() const
{
    return Bootstrap_->GetLegacyMasterConnector();
}

bool TBootstrapBase::UseNewHeartbeats() const
{
    return Bootstrap_->UseNewHeartbeats();
}

void TBootstrapBase::ResetAndRegisterAtMaster()
{
    return Bootstrap_->ResetAndRegisterAtMaster();
}

bool TBootstrapBase::IsConnected() const
{
    return Bootstrap_->IsConnected();
}

TNodeId TBootstrapBase::GetNodeId() const
{
    return Bootstrap_->GetNodeId();
}

TString TBootstrapBase::GetLocalHostName() const
{
    return Bootstrap_->GetLocalHostName();
}

TMasterEpoch TBootstrapBase::GetMasterEpoch() const
{
    return Bootstrap_->GetMasterEpoch();
}

const TNodeDirectoryPtr& TBootstrapBase::GetNodeDirectory() const
{
    return Bootstrap_->GetNodeDirectory();
}

TNetworkPreferenceList TBootstrapBase::GetLocalNetworks() const
{
    return Bootstrap_->GetLocalNetworks();
}

std::optional<TString> TBootstrapBase::GetDefaultNetworkName() const
{
    return Bootstrap_->GetDefaultNetworkName();
}

TString TBootstrapBase::GetDefaultLocalAddressOrThrow() const
{
    return Bootstrap_->GetDefaultLocalAddressOrThrow();
}

const NHttp::IServerPtr& TBootstrapBase::GetHttpServer() const
{
    return Bootstrap_->GetHttpServer();
}

const NRpc::IServerPtr& TBootstrapBase::GetRpcServer() const
{
    return Bootstrap_->GetRpcServer();
}

const IBlockCachePtr& TBootstrapBase::GetBlockCache() const
{
    return Bootstrap_->GetBlockCache();
}

const IClientBlockCachePtr& TBootstrapBase::GetClientBlockCache() const
{
    return Bootstrap_->GetClientBlockCache();
}

const IChunkMetaManagerPtr& TBootstrapBase::GetChunkMetaManager() const
{
    return Bootstrap_->GetChunkMetaManager();
}

const TChunkReaderSweeperPtr& TBootstrapBase::GetChunkReaderSweeper() const
{
    return Bootstrap_->GetChunkReaderSweeper();
}

const IVersionedChunkMetaManagerPtr& TBootstrapBase::GetVersionedChunkMetaManager() const
{
    return Bootstrap_->GetVersionedChunkMetaManager();
}

const IMapNodePtr& TBootstrapBase::GetOrchidRoot() const
{
    return Bootstrap_->GetOrchidRoot();
}

bool TBootstrapBase::IsReadOnly() const
{
    return Bootstrap_->IsReadOnly();
}

bool TBootstrapBase::IsDecommissioned() const
{
    return Bootstrap_->IsDecommissioned();
}

TNetworkStatistics& TBootstrapBase::GetNetworkStatistics() const
{
    return Bootstrap_->GetNetworkStatistics();
}

const IChunkRegistryPtr& TBootstrapBase::GetChunkRegistry() const
{
    return Bootstrap_->GetChunkRegistry();
}

const IBlobReaderCachePtr& TBootstrapBase::GetBlobReaderCache() const
{
    return Bootstrap_->GetBlobReaderCache();
}

const TJobControllerPtr& TBootstrapBase::GetJobController() const
{
    return Bootstrap_->GetJobController();
}

EJobEnvironmentType TBootstrapBase::GetJobEnvironmentType() const
{
    return Bootstrap_->GetJobEnvironmentType();
}

const IIOTrackerPtr& TBootstrapBase::GetIOTracker() const
{
    return Bootstrap_->GetIOTracker();
}

const THashSet<ENodeFlavor>& TBootstrapBase::GetFlavors() const
{
    return Bootstrap_->GetFlavors();
}

bool TBootstrapBase::IsDataNode() const
{
    return Bootstrap_->IsDataNode();
}

bool TBootstrapBase::IsExecNode() const
{
    return Bootstrap_->IsExecNode();
}

bool TBootstrapBase::IsCellarNode() const
{
    return Bootstrap_->IsCellarNode();
}

bool TBootstrapBase::IsTabletNode() const
{
    return Bootstrap_->IsTabletNode();
}

bool TBootstrapBase::IsChaosNode() const
{
    return Bootstrap_->IsChaosNode();
}

NCellarNode::IBootstrap* TBootstrapBase::GetCellarNodeBootstrap() const
{
    return Bootstrap_->GetCellarNodeBootstrap();
}

NDataNode::IBootstrap* TBootstrapBase::GetDataNodeBootstrap() const
{
    return Bootstrap_->GetDataNodeBootstrap();
}

NExecNode::IBootstrap* TBootstrapBase::GetExecNodeBootstrap() const
{
    return Bootstrap_->GetExecNodeBootstrap();
}

NChaosNode::IBootstrap* TBootstrapBase::GetChaosNodeBootstrap() const
{
    return Bootstrap_->GetChaosNodeBootstrap();
}

NTabletNode::IBootstrap* TBootstrapBase::GetTabletNodeBootstrap() const
{
    return Bootstrap_->GetTabletNodeBootstrap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
