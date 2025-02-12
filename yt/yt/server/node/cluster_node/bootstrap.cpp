#include "bootstrap.h"

#include "config.h"
#include "proxying_chunk_service.h"
#include "dynamic_config_manager.h"
#include "node_resource_manager.h"
#include "master_connector.h"
#include "private.h"

#include <yt/yt/server/node/cellar_node/bootstrap.h>
#include <yt/yt/server/node/cellar_node/config.h>
#include <yt/yt/server/node/cellar_node/bundle_dynamic_config_manager.h>
#include <yt/yt/server/node/cellar_node/master_connector.h>

#include <yt/yt/server/node/chaos_node/bootstrap.h>
#include <yt/yt/server/node/chaos_node/slot_manager.h>

#include <yt/yt/server/node/data_node/blob_reader_cache.h>
#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/chunk_registry.h>
#include <yt/yt/server/node/data_node/chunk_reader_sweeper.h>
#include <yt/yt/server/node/data_node/chunk_store.h>
#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/data_node_service.h>
#include <yt/yt/server/node/data_node/job.h>
#include <yt/yt/server/node/data_node/journal_dispatcher.h>
#include <yt/yt/server/node/data_node/location.h>
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
#include <yt/yt/server/node/exec_node/job_prober_service.h>
#include <yt/yt/server/node/exec_node/master_connector.h>
#include <yt/yt/server/node/exec_node/private.h>
#include <yt/yt/server/node/exec_node/scheduler_connector.h>
#include <yt/yt/server/node/exec_node/slot_manager.h>
#include <yt/yt/server/node/exec_node/supervisor_service.h>

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

#include <yt/yt/server/node/job_agent/job_resource_manager.h>

#include <yt/yt/server/node/query_agent/query_executor.h>
#include <yt/yt/server/node/query_agent/query_service.h>

#include <yt/yt/server/lib/cellar_agent/bootstrap_proxy.h>
#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>
#include <yt/yt/server/lib/cellar_agent/config.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/misc/address_helpers.h>
#include <yt/yt/server/lib/misc/job_reporter.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/transaction_server/timestamp_proxy_service.h>

#include <yt/yt/server/lib/admin/admin_service.h>
#include <yt/yt/server/lib/admin/restart_service.h>

#include <yt/yt/server/lib/io/config.h>
#include <yt/yt/server/lib/io/io_tracker.h>

#include <yt/yt/server/lib/hydra/snapshot.h>

#include <yt/yt/server/lib/misc/bootstrap.h>

#ifdef __linux__
#include <yt/yt/library/containers/instance.h>
#include <yt/yt/library/containers/instance_limits_tracker.h>
#include <yt/yt/library/containers/porto_executor.h>
#include <yt/yt/library/containers/container_devices_checker.h>
#endif

#include <yt/yt/library/fusion/service_locator.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory.h>
#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/chaos_client/config.h>
#include <yt/yt/ytlib/chaos_client/chaos_residency_cache.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/chunk_replica_cache.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/ytlib/hive/cell_directory.h>
#include <yt/yt/ytlib/hive/cell_directory_synchronizer.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/object_client/config.h>
#include <yt/yt/ytlib/object_client/caching_object_service.h>
#include <yt/yt/ytlib/object_client/object_service_cache.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>
#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/library/stockpile/config.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/transaction_client/config.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>
#include <yt/yt/client/transaction_client/remote_timestamp_provider.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>
#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/fair_throttler.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>
#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/channel.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/virtual.h>

#include <util/system/fs.h>

namespace NYT::NClusterNode {

using namespace NAdmin;
using namespace NApi;
using namespace NAuth;
using namespace NBus;
using namespace NCellarAgent;
using namespace NCellarClient;
using namespace NCellMasterClient;
using namespace NChaosNode;
using namespace NChunkClient;
using namespace NContainers;
using namespace NFusion;
using namespace NNodeTrackerClient;
using namespace NConcurrency;
using namespace NDataNode;
using namespace NElection;
using namespace NExecNode;
using namespace NHiveClient;
using namespace NHiveServer;
using namespace NHydra;
using namespace NIO;
using namespace NJobAgent;
using namespace NJobProxy;
using namespace NMonitoring;
using namespace NNet;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NOrchid;
using namespace NProfiling;
using namespace NQueryAgent;
using namespace NRpc;
using namespace NScheduler;
using namespace NTableClient;
using namespace NTabletNode;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NThreading;
using namespace NYTree;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "NodeBoot");

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(NNodeTrackerClient::TNodeId nodeId), MasterConnected);
    DEFINE_SIGNAL_OVERRIDE(void(), MasterDisconnected);
    DEFINE_SIGNAL_OVERRIDE(void(std::vector<TError>* alerts), PopulateAlerts);
    DEFINE_SIGNAL_OVERRIDE(void(const TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs), SecondaryMasterCellListChanged);
    DEFINE_SIGNAL_OVERRIDE(void(const TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs), ReadyToUpdateHeartbeatStream);

public:
    TBootstrap(
        TClusterNodeBootstrapConfigPtr config,
        INodePtr configNode,
        IServiceLocatorPtr serviceLocator)
        : Config_(std::move(config))
        , ConfigNode_(std::move(configNode))
        , ServiceLocator_(std::move(serviceLocator))
        , ControlActionQueue_(New<TActionQueue>("Control"))
        , JobActionQueue_(New<TActionQueue>("Job"))
        , JobInvoker_(CreateWatchdogInvoker(
            JobActionQueue_->GetInvoker(),
            ClusterNodeLogger(),
            TDuration::MilliSeconds(500)))
    { }

    void Initialize() final
    {
        BIND(&TBootstrap::DoInitialize, MakeStrong(this))
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

    TFuture<void> Run() final
    {
        return BIND(&TBootstrap::DoRun, MakeStrong(this))
            .AsyncVia(GetControlInvoker())
            .Run();
    }

    const IMasterConnectorPtr& GetMasterConnector() const override
    {
        return MasterConnector_;
    }

    NDiskManager::IHotswapManagerPtr TryGetHotswapManager() const override
    {
        return ServiceLocator_->FindService<NDiskManager::IHotswapManagerPtr>();
    }

    TRelativeThroughputThrottlerConfigPtr PatchRelativeNetworkThrottlerConfig(
        const TRelativeThroughputThrottlerConfigPtr& config) const override
    {
        // NB: Absolute value limit suppresses relative one.
        if (config->Limit || !config->RelativeLimit) {
            return config;
        }

        auto patchedConfig = CloneYsonStruct(config);
        patchedConfig->Limit = *config->RelativeLimit * Config_->NetworkBandwidth;

        return patchedConfig;
    }

    void SetDecommissioned(bool decommissioned) override
    {
        Decommissioned_ = decommissioned;
    }

    // IBootstrapBase implementation.
    const INodeMemoryTrackerPtr& GetNodeMemoryUsageTracker() const override
    {
        return NodeMemoryUsageTracker_;
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

    const NConcurrency::IThroughputThrottlerPtr& GetUserJobContainerCreationThrottler() const override
    {
        return UserJobContainerCreationThrottler_;
    }

    const NConcurrency::IThroughputThrottlerPtr& GetAnnounceChunkReplicaRpsOutThrottler() const override
    {
        return AnnounceChunkReplicaRpsOutThrottler_;
    }

    const TBufferedProducerPtr& GetBufferedProducer() const override
    {
        return BufferedProducer_;
    }

    const TClusterNodeBootstrapConfigPtr& GetConfig() const override
    {
        return Config_;
    }

    const NClusterNode::TClusterNodeDynamicConfigManagerPtr& GetDynamicConfigManager() const override
    {
        return DynamicConfigManager_;
    }

    const NCellarNode::TBundleDynamicConfigManagerPtr& GetBundleDynamicConfigManager() const override
    {
        return BundleDynamicConfigManager_;
    }

    const IInvokerPtr& GetControlInvoker() const override
    {
        return ControlActionQueue_->GetInvoker();
    }

    const IInvokerPtr& GetJobInvoker() const override
    {
        return JobInvoker_;
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

    const NApi::NNative::IClientPtr& GetClient() const override
    {
        return Client_;
    }

    const NApi::NNative::IConnectionPtr& GetConnection() const override
    {
        return Connection_;
    }

    const IAuthenticatorPtr& GetNativeAuthenticator() const override
    {
        return NativeAuthenticator_;
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
        return PrimaryMaster_->CellId;
    }

    TCellId GetCellId(TCellTag cellTag) const override
    {
        return cellTag == PrimaryMasterCellTagSentinel
            ? GetCellId()
            : ReplaceCellTagInId(GetCellId(), cellTag);
    }

    std::vector<std::string> GetMasterAddressesOrThrow(TCellTag cellTag) const override
    {
        // TODO(babenko): handle service discovery.
        auto unwrapAddresses = [&] (const auto& optionalAddresses) {
            if (!optionalAddresses) {
                THROW_ERROR_EXCEPTION("Missing addresses for master cell with tag %v", cellTag);
            }
            return *optionalAddresses;
        };

        auto cellId = GetCellId(cellTag);

        if (GetCellId() == cellId) {
            return unwrapAddresses(PrimaryMaster_->Addresses);
        }

        const auto& secondaryMasterConnectionConfigs = GetSecondaryMasterConnectionConfigs();
        auto secondaryMasterIt = secondaryMasterConnectionConfigs.find(cellTag);
        if (secondaryMasterIt == secondaryMasterConnectionConfigs.end()) {
            THROW_ERROR_EXCEPTION("Master with cell tag %v is not known", cellTag);
        }
        const auto& secondaryMaster = secondaryMasterIt->second;
        return unwrapAddresses(secondaryMaster->Addresses);
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

    std::string GetLocalHostName() const override
    {
        return MasterConnector_->GetLocalHostName();
    }

    TMasterEpoch GetMasterEpoch() const override
    {
        return MasterConnector_->GetEpoch();
    }

    const TNodeDirectoryPtr& GetNodeDirectory() const override
    {
        return Connection_->GetNodeDirectory();
    }

    TNetworkPreferenceList GetNetworks() const override
    {
        return Connection_->GetNetworks();
    }

    TNetworkPreferenceList GetLocalNetworks() const override
    {
        return Config_->Addresses.empty()
            ? DefaultNetworkPreferences
            : GetIths<0>(Config_->Addresses);
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

    const IMemoryUsageTrackerPtr& GetRpcMemoryUsageTracker() const override
    {
        return RpcMemoryUsageTracker_;
    }

    const IMemoryUsageTrackerPtr& GetReadBlockMemoryUsageTracker() const override
    {
        return ReadBlockMemoryUsageTracker_;
    }

    const IMemoryUsageTrackerPtr& GetSystemJobsMemoryUsageTracker() const override
    {
        return SystemJobsMemoryUsageTracker_;
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
        // TODO(gritukan): Make node without dynamic config read-only after YT-12933.
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

    const TJobResourceManagerPtr& GetJobResourceManager() const override
    {
        return JobResourceManager_;
    }

    const TRestartManagerPtr& GetRestartManager() const override
    {
        return RestartManager_;
    }

    const IIOTrackerPtr& GetIOTracker() const override
    {
        return IOTracker_;
    }

    EJobEnvironmentType GetJobEnvironmentType() const override
    {
        const auto& slotManagerConfig = Config_->ExecNode->SlotManager;
        return slotManagerConfig->JobEnvironment.GetCurrentType();
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

    const NClusterNode::IBootstrap* GetClusterNodeBootstrap() const override
    {
        return this;
    }

    bool NeedDataNodeBootstrap() const override
    {
        if (IsDataNode()) {
            return true;
        }

        return IsExecNode() && !Config_->ExecNodeIsNotDataNode;
    }

private:
    const TClusterNodeBootstrapConfigPtr Config_;
    const INodePtr ConfigNode_;
    const IServiceLocatorPtr ServiceLocator_;

    const TActionQueuePtr ControlActionQueue_;
    const TActionQueuePtr JobActionQueue_;
    const IInvokerPtr JobInvoker_;

    IThreadPoolPtr ConnectionThreadPool_;
    IThreadPoolPtr StorageLightThreadPool_;
    IThreadPoolPtr StorageHeavyThreadPool_;
    IPrioritizedInvokerPtr StorageHeavyInvoker_;
    TActionQueuePtr MasterCacheQueue_;

    IMonitoringManagerPtr MonitoringManager_;

    NYT::NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    IMapNodePtr OrchidRoot_;

    INodeMemoryTrackerPtr NodeMemoryUsageTracker_;
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

    IReconfigurableThroughputThrottlerPtr RawUserJobContainerCreationThrottler_;
    IThroughputThrottlerPtr UserJobContainerCreationThrottler_;

    IReconfigurableThroughputThrottlerPtr RawAnnounceChunkReplicaRpsOutThrottler_;
    IThroughputThrottlerPtr AnnounceChunkReplicaRpsOutThrottler_;

#ifdef __linux__
    NContainers::TInstanceLimitsTrackerPtr InstanceLimitsTracker_;

    TActionQueuePtr ContainerDevicesCheckerQueue_;
    NContainers::TContainerDevicesCheckerPtr ContainerDevicesChecker_;
#endif

    TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    NCellarNode::TBundleDynamicConfigManagerPtr BundleDynamicConfigManager_;

    NApi::NNative::IClientPtr Client_;
    NApi::NNative::IConnectionPtr Connection_;
    IAuthenticatorPtr NativeAuthenticator_;

    TJobResourceManagerPtr JobResourceManager_;

    TRestartManagerPtr RestartManager_;

    IMasterConnectorPtr MasterConnector_;

    IMemoryUsageTrackerPtr ReadBlockMemoryUsageTracker_;
    IMemoryUsageTrackerPtr RpcMemoryUsageTracker_;
    IMemoryUsageTrackerPtr SystemJobsMemoryUsageTracker_;

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
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, CachingObjectServicesLock_);
    THashMap<TCellTag, ICachingObjectServicePtr> CachingObjectServices_;
    THashMap<TCellTag, IServicePtr> ProxyingChunkServices_;

    NApi::NNative::TMasterConnectionConfigPtr PrimaryMaster_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SecondaryMasterConnectionLock_);
    TSecondaryMasterConnectionConfigs SecondaryMasterConnectionConfigs_;

    IIOTrackerPtr IOTracker_;

    THashSet<ENodeFlavor> Flavors_;

    NCellarNode::IBootstrapPtr CellarNodeBootstrap_;
    NChaosNode::IBootstrapPtr ChaosNodeBootstrap_;
    NExecNode::IBootstrapPtr ExecNodeBootstrap_;
    NDataNode::IBootstrapPtr DataNodeBootstrap_;
    NTabletNode::IBootstrapPtr TabletNodeBootstrap_;

    bool Decommissioned_ = false;

    void DoRun()
    {
        DoInitialize();
        DoStart();
    }

    void DoInitialize()
    {
        auto localRpcAddresses = GetLocalAddresses(Config_->Addresses, Config_->RpcPort);

        if (!Config_->ClusterConnection->Static->Networks) {
            Config_->ClusterConnection->Static->Networks = GetLocalNetworks();
        }

        {
            const auto& flavors = Config_->Flavors;
            Flavors_ = THashSet<ENodeFlavor>(flavors.begin(), flavors.end());
        }

        PrimaryMaster_ = Config_->ClusterConnection->Static->PrimaryMaster;
        for (const auto& secondaryMaster : Config_->ClusterConnection->Static->SecondaryMasters) {
            EmplaceOrCrash(SecondaryMasterConnectionConfigs_, CellTagFromId(secondaryMaster->CellId), secondaryMaster);
        }

        YT_LOG_INFO(
            "Initializing cluster node (LocalAddresses: %v, PrimaryMasterAddresses: %v, NodeTags: %v, Flavors: %v)",
            GetValues(localRpcAddresses),
            PrimaryMaster_->Addresses,
            Config_->Tags,
            Flavors_);

        NodeMemoryUsageTracker_ = CreateNodeMemoryTracker(
            Config_->ResourceLimits->TotalMemory,
            /*limits*/ {},
            Logger(),
            ClusterNodeProfiler().WithPrefix("/memory_usage"));

        // NB: Connection thread pool is required for dynamic config manager
        // initialization, so it is created before other thread pools.
        ConnectionThreadPool_ = CreateThreadPool(
            Config_->ClusterConnection->Dynamic->ThreadPoolSize,
            "Connection");

        NApi::NNative::TConnectionOptions connectionOptions;
        connectionOptions.ConnectionInvoker = ConnectionThreadPool_->GetInvoker();
        connectionOptions.BlockCache = GetBlockCache();

        Connection_ = NApi::NNative::CreateConnection(
            Config_->ClusterConnection,
            std::move(connectionOptions));

        // Cycles are fine for bootstrap.
        Connection_->GetMasterCellDirectory()->SubscribeCellDirectoryChanged(
            BIND_NO_PROPAGATE(&TBootstrap::OnMasterCellDirectoryChanged, MakeStrong(this)));

        NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(Connection_);

        Client_ = Connection_->CreateNativeClient(
            TClientOptions::FromUser(NSecurityClient::RootUserName));

        NLogging::GetDynamicTableLogWriterFactory()->SetClient(Client_);

        BufferedProducer_ = New<TBufferedProducer>();
        ClusterNodeProfiler().WithProducerRemoveSupport().AddProducer("", BufferedProducer_);

        MasterCacheQueue_ = New<TActionQueue>("MasterCache");
        StorageHeavyThreadPool_ = CreateThreadPool(
            Config_->DataNode->StorageHeavyThreadCount,
            "StorageHeavy");
        StorageHeavyInvoker_ = CreatePrioritizedInvoker(StorageHeavyThreadPool_->GetInvoker(), "bootstrap");
        StorageLightThreadPool_ = CreateThreadPool(
            Config_->DataNode->StorageLightThreadCount,
            "StorageLight");

        if (Config_->EnableFairThrottler) {
            Config_->InThrottler->TotalLimit = GetNetworkThrottlerLimit(nullptr, {});
            InThrottler_ = New<TFairThrottler>(
                Config_->InThrottler,
                ClusterNodeLogger().WithTag("Direction: %v", "In"),
                ClusterNodeProfiler().WithPrefix("/in_throttler"));
            DefaultInThrottler_ = GetInThrottler("default");

            Config_->OutThrottler->TotalLimit = GetNetworkThrottlerLimit(nullptr, {});
            OutThrottler_ = New<TFairThrottler>(
                Config_->OutThrottler,
                ClusterNodeLogger().WithTag("Direction: %v", "Out"),
                ClusterNodeProfiler().WithPrefix("/out_throttler"));
            DefaultOutThrottler_ = GetOutThrottler("default");
        } else {
            auto getThrottlerConfig = [&] (EDataNodeThrottlerKind kind) {
                return PatchRelativeNetworkThrottlerConfig(Config_->DataNode->Throttlers[kind]);
            };

            LegacyRawTotalInThrottler_ = CreateNamedReconfigurableThroughputThrottler(
                getThrottlerConfig(EDataNodeThrottlerKind::TotalIn),
                "TotalIn",
                ClusterNodeLogger(),
                ClusterNodeProfiler().WithPrefix("/throttlers"));
            LegacyTotalInThrottler_ = IThroughputThrottlerPtr(LegacyRawTotalInThrottler_);

            LegacyRawTotalOutThrottler_ = CreateNamedReconfigurableThroughputThrottler(
                getThrottlerConfig(EDataNodeThrottlerKind::TotalOut),
                "TotalOut",
                ClusterNodeLogger(),
                ClusterNodeProfiler().WithPrefix("/throttlers"));
            LegacyTotalOutThrottler_ = IThroughputThrottlerPtr(LegacyRawTotalOutThrottler_);
        }

        RawUserJobContainerCreationThrottler_ = CreateNamedReconfigurableThroughputThrottler(
            New<NConcurrency::TThroughputThrottlerConfig>(),
            "UserJobContainerCreation",
            ClusterNodeLogger(),
            ClusterNodeProfiler().WithPrefix("/user_job_container_creation_throttler"));
        UserJobContainerCreationThrottler_ = IThroughputThrottlerPtr(RawUserJobContainerCreationThrottler_);

        RawReadRpsOutThrottler_ = CreateNamedReconfigurableThroughputThrottler(
            Config_->DataNode->ReadRpsOutThrottler,
            "ReadRpsOut",
            ClusterNodeLogger(),
            ClusterNodeProfiler().WithPrefix("/out_read_rps_throttler"));
        ReadRpsOutThrottler_ = IThroughputThrottlerPtr(RawReadRpsOutThrottler_);

        RawAnnounceChunkReplicaRpsOutThrottler_ = CreateNamedReconfigurableThroughputThrottler(
            Config_->DataNode->AnnounceChunkReplicaRpsOutThrottler,
            "AnnounceChunkReplicaRpsOut",
            ClusterNodeLogger(),
            ClusterNodeProfiler().WithPrefix("/out_announce_chunk_replica_rps_throttler"));
        AnnounceChunkReplicaRpsOutThrottler_ = IThroughputThrottlerPtr(RawAnnounceChunkReplicaRpsOutThrottler_);

        ReadBlockMemoryUsageTracker_ = NodeMemoryUsageTracker_->WithCategory(EMemoryCategory::PendingDiskRead);
        SystemJobsMemoryUsageTracker_ = NodeMemoryUsageTracker_->WithCategory(EMemoryCategory::SystemJobs);
        RpcMemoryUsageTracker_ = NodeMemoryUsageTracker_->WithCategory(EMemoryCategory::Rpc);

        BlockCache_ = ClientBlockCache_ = CreateClientBlockCache(
            Config_->DataNode->BlockCache,
            EBlockType::UncompressedData | EBlockType::CompressedData |
                EBlockType::HashTableChunkIndex | EBlockType::XorFilter | EBlockType::ChunkFragmentsData,
            NodeMemoryUsageTracker_->WithCategory(EMemoryCategory::BlockCache),
            DataNodeProfiler().WithPrefix("/block_cache"));

        BusServer_ = CreateBusServer(
            Config_->BusServer,
            GetYTPacketTranscoderFactory(),
            NodeMemoryUsageTracker_->WithCategory(EMemoryCategory::Rpc));

        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);
        RpcServer_->Configure(Config_->RpcServer);

        InitProxyingChunkService(PrimaryMaster_);
        for (const auto& [_, masterConfig] : SecondaryMasterConnectionConfigs_) {
            InitProxyingChunkService(masterConfig);
        }

        MasterConnector_ = NClusterNode::CreateMasterConnector(
            this,
            localRpcAddresses,
            NServer::GetLocalAddresses(Config_->Addresses, Config_->SkynetHttpPort),
            NServer::GetLocalAddresses(Config_->Addresses, Config_->MonitoringPort),
            Config_->Tags);

        // Cycles are fine for bootstrap.
        MasterConnector_->SubscribePopulateAlerts(BIND_NO_PROPAGATE(&TBootstrap::PopulateAlerts, MakeStrong(this)));
        MasterConnector_->SubscribeMasterConnected(BIND_NO_PROPAGATE(&TBootstrap::OnMasterConnected, MakeStrong(this)));
        MasterConnector_->SubscribeMasterDisconnected(BIND_NO_PROPAGATE(&TBootstrap::OnMasterDisconnected, MakeStrong(this)));

        DynamicConfigManager_ = New<TClusterNodeDynamicConfigManager>(this);
        // Cycles are fine for bootstrap.
        DynamicConfigManager_->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnDynamicConfigChanged, MakeStrong(this)));

        BundleDynamicConfigManager_ = New<NCellarNode::TBundleDynamicConfigManager>(this);
        // Cycles are fine for bootstrap.
        BundleDynamicConfigManager_->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnBundleDynamicConfigChanged, MakeStrong(this)));

        IOTracker_ = CreateIOTracker(DynamicConfigManager_->GetConfig()->IOTracker);

        ChunkRegistry_ = CreateChunkRegistry(this);
        ChunkReaderSweeper_ = New<TChunkReaderSweeper>(
            GetDynamicConfigManager(),
            GetStorageHeavyInvoker());

        ChunkMetaManager_ = CreateChunkMetaManager(
            Config_->DataNode,
            GetDynamicConfigManager(),
            GetNodeMemoryUsageTracker());

        BlobReaderCache_ = CreateBlobReaderCache(
            GetConfig()->DataNode,
            GetDynamicConfigManager(),
            ChunkMetaManager_);

        VersionedChunkMetaManager_ = CreateVersionedChunkMetaManager(
            Config_->TabletNode->VersionedChunkMetaCache,
            GetNodeMemoryUsageTracker()->WithCategory(EMemoryCategory::VersionedChunkMeta));

        NetworkStatistics_ = std::make_unique<TNetworkStatistics>(Config_->DataNode);

        NodeResourceManager_ = New<TNodeResourceManager>(this);

        auto localAddress = GetDefaultAddress(localRpcAddresses);

        JobResourceManager_ = TJobResourceManager::CreateJobResourceManager(this);

        RestartManager_ = New<TRestartManager>(GetControlInvoker());

        auto timestampProviderConfig = Config_->TimestampProvider;
        if (!timestampProviderConfig) {
            timestampProviderConfig = CreateRemoteTimestampProviderConfig(PrimaryMaster_);
        }
        auto timestampProvider = CreateBatchingRemoteTimestampProvider(
            timestampProviderConfig,
            Connection_->GetChannelFactory());

        RpcServer_->RegisterService(CreateTimestampProxyService(
            timestampProvider,
            /*alienProviders*/ {},
            /*authenticator*/ nullptr));

        RpcServer_->RegisterService(CreateRestartService(
            RestartManager_,
            GetControlInvoker(),
            ClusterNodeLogger(),
            NativeAuthenticator_));

        ObjectServiceCache_ = New<TObjectServiceCache>(
            Config_->CachingObjectService,
            NodeMemoryUsageTracker_->WithCategory(EMemoryCategory::MasterCache),
            Logger(),
            ClusterNodeProfiler().WithPrefix("/object_service_cache"));

        {
            auto guard = WriterGuard(CachingObjectServicesLock_);
            InitCachingObjectService(PrimaryMaster_->CellId);
            for (const auto& [_, masterConfig] : SecondaryMasterConnectionConfigs_) {
                InitCachingObjectService(masterConfig->CellId);
            }
        }

        // NB: Data Node master connector is required for chunk cache.
        if (NeedDataNodeBootstrap()) {
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

        RpcServer_->RegisterService(CreateAdminService(
            GetControlInvoker(),
            ServiceLocator_->FindService<NCoreDump::ICoreDumperPtr>(),
            NativeAuthenticator_));

    #ifdef __linux__
        auto topLevelPortoConfig = Config_->PortoEnvironment;
        // COMPAT(ponasenko-rs): Remove after migration to top-level porto_environment.
        auto execNodePortoConfig = Config_->ExecNode->SlotManager->JobEnvironment.TryGetConcrete<EJobEnvironmentType::Porto>();

        if (topLevelPortoConfig || execNodePortoConfig) {
            auto portoExecutor = CreatePortoExecutor(topLevelPortoConfig
                    ? topLevelPortoConfig->PortoExecutor
                    : execNodePortoConfig->PortoExecutor,
                "limits_tracker");

            // Cycles are fine for bootstrap.
            portoExecutor->SubscribeFailed(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
                YT_LOG_ERROR(error, "Porto executor failed");
                ExecNodeBootstrap_->GetSlotManager()->OnPortoExecutorFailed(error);
            }));

            auto useDaemonSubcontainer = topLevelPortoConfig
                ? topLevelPortoConfig->UseDaemonSubcontainer
                : execNodePortoConfig->UseDaemonSubcontainer;

            auto self = GetSelfPortoInstance(portoExecutor);
            if (Config_->InstanceLimitsUpdatePeriod) {
                auto root = GetRootPortoInstance(portoExecutor);
                auto instance = useDaemonSubcontainer
                    ? GetPortoInstance(portoExecutor, *self->GetParentName())
                    : self;

                InstanceLimitsTracker_ = New<TInstanceLimitsTracker>(
                    instance,
                    root,
                    GetControlInvoker(),
                    *Config_->InstanceLimitsUpdatePeriod);

                // Cylces are fine for bootstrap.
                InstanceLimitsTracker_->SubscribeLimitsUpdated(BIND([this, this_ = MakeStrong(this)] (const NContainers::TInstanceLimits& limits) {
                    NodeResourceManager_->OnInstanceLimitsUpdated(limits);

                    auto config = GetDynamicConfigManager()->GetConfig();
                    ReconfigureThrottlers(config, limits.NetTx, limits.NetRx);
                })
                .Via(GetControlInvoker()));
            }

            if (useDaemonSubcontainer) {
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

            if (IsExecNode()) {
                ContainerDevicesCheckerQueue_ = New<TActionQueue>("ContainerDevicesCheck");
                ContainerDevicesChecker_ = CreateContainerDevicesChecker(
                    NFS::CombinePaths(NFs::CurrentWorkingDirectory(), "test_containers"),
                    New<TPortoExecutorDynamicConfig>(),
                    ContainerDevicesCheckerQueue_->GetInvoker(),
                    Logger());
            }
        }
    #endif

        MasterConnector_->Initialize();

        if (NeedDataNodeBootstrap()) {
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

        JobResourceManager_->Initialize();

        YT_LOG_INFO("Cluster node initialization completed");
    }

    void DoStart()
    {
        auto localRpcAddresses = GetLocalAddresses(Config_->Addresses, Config_->RpcPort);

        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        NMonitoring::Initialize(
            HttpServer_,
            ServiceLocator_->GetServiceOrThrow<TSolomonExporterPtr>(),
            &MonitoringManager_,
            &OrchidRoot_);

        YT_LOG_INFO(
            "Starting node (LocalAddresses: %v, PrimaryMasterAddresses: %v, NodeTags: %v)",
            GetValues(localRpcAddresses),
            PrimaryMaster_->Addresses,
            Config_->Tags);

        // Do not start subsystems until everything is initialized.
        // Start MasterConnector to register at Master.
        MasterConnector_->Start();

        {
            YT_LOG_INFO("Loading dynamic config for the first time");
            auto error = WaitFor(DynamicConfigManager_->GetConfigLoadedFuture());
            YT_LOG_FATAL_UNLESS(
                error.IsOK(),
                error,
                "Unexpected failure while waiting for the first dynamic config loaded");
            YT_LOG_INFO("Dynamic config loaded");
        }

        DoValidateConfig();

        if (IsCellarNode() || IsTabletNode()) {
            BundleDynamicConfigManager_->Start();
        }

        NodeResourceManager_->Start();

        JobResourceManager_->Start();

        // Force start node directory synchronizer.
        Connection_->GetNodeDirectorySynchronizer()->Start();

        Connection_->GetClusterDirectorySynchronizer()->Start();

        Connection_->GetMasterCellDirectorySynchronizer()->Start();

        if (Config_->ExposeConfigInOrchid) {
            SetNodeByYPath(
                OrchidRoot_,
                "/config",
                CreateVirtualNode(ConfigNode_));
            SetNodeByYPath(
                OrchidRoot_,
                "/dynamic_config_manager",
                CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));
            SetNodeByYPath(
                OrchidRoot_,
                "/cluster_connection",
                CreateVirtualNode(Connection_->GetOrchidService()));
            SetNodeByYPath(
                OrchidRoot_,
                "/bundle_dynamic_config_manager",
                CreateVirtualNode(BundleDynamicConfigManager_->GetOrchidService()));
            SetNodeByYPath(
                OrchidRoot_,
                "/connected_secondary_masters",
                CreateVirtualNode(GetSecondaryMasterConnectionConfigsOrchidService()));
        }
        SetNodeByYPath(
            OrchidRoot_,
            "/restart_manager",
            CreateVirtualNode(RestartManager_->GetOrchidService()));
        SetNodeByYPath(
            OrchidRoot_,
            "/object_service_cache",
            CreateVirtualNode(ObjectServiceCache_->GetOrchidService()));
        SetNodeByYPath(
            OrchidRoot_,
            "/node_resource_manager",
            CreateVirtualNode(NodeResourceManager_->GetOrchidService()));
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
            GetControlInvoker(),
            NativeAuthenticator_));

        if (IsCellarNode()) {
            CellarNodeBootstrap_->Run();
        }

        if (IsChaosNode()) {
            ChaosNodeBootstrap_->Run();
        }

        if (NeedDataNodeBootstrap()) {
            DataNodeBootstrap_->Run();
        }

        if (IsExecNode()) {
            ExecNodeBootstrap_->Run();
        }

        if (IsTabletNode()) {
            TabletNodeBootstrap_->Run();
        }

        YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
        RpcServer_->Start();

        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

#ifdef __linux__
        if (ContainerDevicesChecker_) {
            ContainerDevicesChecker_->SubscribeCheck(BIND_NO_PROPAGATE(&TSlotManager::OnContainerDevicesCheckFinished, ExecNodeBootstrap_->GetSlotManager())
                .Via(GetControlInvoker()));
            ContainerDevicesChecker_->Start();
        }
#endif

        YT_LOG_INFO("Node started successfully");
    }

    IYPathServicePtr GetSecondaryMasterConnectionConfigsOrchidService()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return IYPathService::FromProducer(BIND([this, this_ = MakeStrong(this)] (NYson::IYsonConsumer* consumer) {
            auto secondaryMasterConnectionConfigs = IsConnected()
                ? GetSecondaryMasterConnectionConfigs()
                : TSecondaryMasterConnectionConfigs();
            BuildYsonFluently(consumer)
                .Value(secondaryMasterConnectionConfigs);
        }))->Via(GetControlInvoker());
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

    void CompleteNodeRegistration() override
    {
        YT_ASSERT_INVOKER_AFFINITY(GetControlInvoker());

        if (DynamicConfigManager_->GetConfigLoadedFuture().IsSet()) {
            return;
        }

        // We must start DynamicConfigManager after we have been registered at master
        // so that we can fetch correct tags and properly apply dynamic config.
        DynamicConfigManager_->Start();
        {
            auto error = WaitFor(DynamicConfigManager_->GetConfigLoadedFuture());

            YT_LOG_FATAL_UNLESS(
                error.IsOK(),
                error,
                "Unexpected failure while waiting for the first dynamic config loaded during node registration compelition");
        }
    }

    void ReconfigureFairThrottlers(
        const TClusterNodeDynamicConfigPtr& newConfig,
        std::optional<i64> netTxLimit,
        std::optional<i64> netRxLimit)
    {
        auto throttlerConfig = New<TFairThrottlerConfig>();
        throttlerConfig->TotalLimit = GetNetworkThrottlerLimit(newConfig, netRxLimit);

        THashMap<TString, TFairThrottlerBucketConfigPtr> inBucketsConfig;
        for (const auto& bucket : EnabledInThrottlers_) {
            inBucketsConfig[bucket] = Config_->InThrottlers[bucket];
            if (newConfig->InThrottlers[bucket]) {
                inBucketsConfig[bucket] = newConfig->InThrottlers[bucket];
            }
        }
        InThrottler_->Reconfigure(throttlerConfig, inBucketsConfig);

        throttlerConfig->TotalLimit = GetNetworkThrottlerLimit(newConfig, netTxLimit);
        THashMap<TString, TFairThrottlerBucketConfigPtr> outBucketsConfig;
        for (const auto& bucket : EnabledOutThrottlers_) {
            outBucketsConfig[bucket] = Config_->OutThrottlers[bucket];
            if (newConfig->OutThrottlers[bucket]) {
                outBucketsConfig[bucket] = newConfig->OutThrottlers[bucket];
            }
        }
        OutThrottler_->Reconfigure(throttlerConfig, outBucketsConfig);
    }

    void ReconfigureCaches(
        const NCellarNode::TBundleDynamicConfigPtr& bundleConfig,
        const TClusterNodeDynamicConfigPtr& nodeConfig)
    {
        auto overrideCapacity = [] (const TSlruCacheDynamicConfigPtr& config, std::optional<i64> capacity) {
            if (capacity) {
                config->Capacity = capacity;
            }
        };

        auto blockCacheConfig = CloneYsonStruct(nodeConfig->DataNode->BlockCache);
        auto versionedChunkMetaConfig = CloneYsonStruct(nodeConfig->TabletNode->VersionedChunkMetaCache);

        const auto& memoryLimits = bundleConfig->MemoryLimits;
        overrideCapacity(blockCacheConfig->CompressedData, memoryLimits->CompressedBlockCache);
        overrideCapacity(blockCacheConfig->UncompressedData, memoryLimits->UncompressedBlockCache);
        overrideCapacity(blockCacheConfig->XorFilter, memoryLimits->KeyFilterBlockCache);
        overrideCapacity(versionedChunkMetaConfig, memoryLimits->VersionedChunkMeta);

        ClientBlockCache_->Reconfigure(blockCacheConfig);
        VersionedChunkMetaManager_->Reconfigure(versionedChunkMetaConfig);
    }

    void OnBundleDynamicConfigChanged(
        const NCellarNode::TBundleDynamicConfigPtr& /*oldConfig*/,
        const NCellarNode::TBundleDynamicConfigPtr& newConfig)
    {
        auto nodeConfig = GetDynamicConfigManager()->GetConfig();
        ReconfigureCaches(newConfig, nodeConfig);
    }

    void ReconfigureThrottlers(
        const TClusterNodeDynamicConfigPtr& newConfig,
        std::optional<i64> netTxLimit,
        std::optional<i64> netRxLimit)
    {
        if (Config_->EnableFairThrottler) {
            ReconfigureFairThrottlers(newConfig, netTxLimit, netRxLimit);
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
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& oldConfig,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        if (auto stockpile = newConfig->GetSingletonConfig<TStockpileDynamicConfig>();
            stockpile->TotalMemoryFractionOverride)
        {
            stockpile->BufferSize = NodeMemoryUsageTracker_->GetTotalLimit() *
                (*stockpile->TotalMemoryFractionOverride);
        }
        TSingletonManager::Reconfigure(newConfig);

        StorageHeavyThreadPool_->SetThreadCount(
            newConfig->DataNode->StorageHeavyThreadCount.value_or(Config_->DataNode->StorageHeavyThreadCount));
        StorageLightThreadPool_->SetThreadCount(
            newConfig->DataNode->StorageLightThreadCount.value_or(Config_->DataNode->StorageLightThreadCount));

        auto netTxLimit = NodeResourceManager_->GetNetTxLimit();
        auto netRxLimit = NodeResourceManager_->GetNetRxLimit();
        ReconfigureThrottlers(newConfig, netTxLimit, netRxLimit);

        RawReadRpsOutThrottler_->Reconfigure(newConfig->DataNode->ReadRpsOutThrottler
            ? newConfig->DataNode->ReadRpsOutThrottler
            : Config_->DataNode->ReadRpsOutThrottler);
        RawAnnounceChunkReplicaRpsOutThrottler_->Reconfigure(newConfig->DataNode->AnnounceChunkReplicaRpsOutThrottler
            ? newConfig->DataNode->AnnounceChunkReplicaRpsOutThrottler
            : Config_->DataNode->AnnounceChunkReplicaRpsOutThrottler);
        RawUserJobContainerCreationThrottler_->Reconfigure(newConfig->ExecNode->UserJobContainerCreationThrottler);

        RpcServer_->OnDynamicConfigChanged(newConfig->RpcServer);

        ObjectServiceCache_->Reconfigure(newConfig->CachingObjectService);
        for (const auto& [_, service] : GetCachingObjectServices()) {
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

        if (ContainerDevicesChecker_ && newConfig->ExecNode->SlotManager) {
            const auto& environmentConfig = newConfig->ExecNode->SlotManager->JobEnvironment;

            TPortoExecutorDynamicConfigPtr portoExecutor = nullptr;
            if (auto portoConfig = newConfig->PortoEnvironment) {
                portoExecutor = portoConfig->PortoExecutor;
            } else if (auto portoConfig = environmentConfig.TryGetConcrete<EJobEnvironmentType::Porto>()) {
                // COMPAT(ponasenko-rs): Remove after migration to top-level porto_environment.
                portoExecutor = portoConfig->PortoExecutor;
            }

            if (portoExecutor) {
                ContainerDevicesChecker_->OnDynamicConfigChanged(portoExecutor);
            }
        }
    #endif

        auto bundleConfig = GetBundleDynamicConfigManager()->GetConfig();
        ReconfigureCaches(bundleConfig, newConfig);

        JobResourceManager_->OnDynamicConfigChanged(
            oldConfig,
            newConfig);

        auto newChunkReplicaCacheConfig = CloneYsonStruct(Config_->ClusterConnection->Dynamic->ChunkReplicaCache);
        UpdateYsonStructField(
            newChunkReplicaCacheConfig->ExpirationTime,
            newConfig->ChunkReplicaCache->ExpirationTime);
        Connection_->GetChunkReplicaCache()->Reconfigure(std::move(newChunkReplicaCacheConfig));

        auto newChaosResidencyCacheConfig = CloneYsonStruct(Config_->ClusterConnection->Dynamic->ChaosResidencyCache);
        UpdateYsonStructField(
            newChaosResidencyCacheConfig->EnableClientMode,
            newConfig->ChaosResidencyCache->EnableClientMode);
        Connection_->GetChaosResidencyCache()->Reconfigure(std::move(newChaosResidencyCacheConfig));
    }

    void PopulateAlerts(std::vector<TError>* alerts)
    {
        PopulateAlerts_.Fire(alerts);

        PopulateTotalMemoryAlerts(alerts);

        PopulatePerCategoryMemoryAlerts(alerts);

        if (!UnrecognizedOptionsAlert_.IsOK()) {
            alerts->push_back(UnrecognizedOptionsAlert_);
        }
    }

    void PopulateTotalMemoryAlerts(std::vector<TError>* alerts)
    {
        // NB: Don't expect IsXXXExceeded helpers to be atomic.
        auto totalUsed = NodeMemoryUsageTracker_->GetTotalUsed();
        auto totalLimit = NodeMemoryUsageTracker_->GetTotalLimit();

        if (DynamicConfigManager_->GetConfig()->TotalMemoryLimitExceededThreshold * totalUsed > totalLimit) {
            alerts->push_back(TError(NChunkClient::EErrorCode::TotalMemoryLimitExceeded, "Total memory limit exceeded")
                << TErrorAttribute("used", totalUsed)
                << TErrorAttribute("limit", totalLimit));
        } else if (DynamicConfigManager_->GetConfig()->MemoryUsageIsCloseToLimitThreshold * totalUsed > totalLimit) {
            alerts->push_back(TError("Memory usage is close to the limit")
                << TErrorAttribute("used", totalUsed)
                << TErrorAttribute("limit", totalLimit));
        }
    }

    void PopulatePerCategoryMemoryAlerts(std::vector<TError>* alerts)
    {
        for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
            auto used = NodeMemoryUsageTracker_->GetUsed(category);
            auto limit = NodeMemoryUsageTracker_->GetLimit(category);
            if (used > limit * DynamicConfigManager_->GetConfig()->MemoryLimitExceededForCategoryThreshold) {
                alerts->push_back(TError("Memory limit exceeded for category %Qlv",
                    category)
                    << TErrorAttribute("used", used)
                    << TErrorAttribute("limit", limit));
            }
        }
    }

    void OnMasterConnected(TNodeId nodeId)
    {
        MasterConnected_.Fire(nodeId);

        for (const auto& [_, cachingObjectService] : GetCachingObjectServices()) {
            RpcServer_->RegisterService(cachingObjectService);
        }
    }

    void OnMasterDisconnected()
    {
        MasterDisconnected_.Fire();

        for (const auto& [_, cachingObjectService] : GetCachingObjectServices()) {
            RpcServer_->UnregisterService(cachingObjectService);
        }
    }

    THashMap<TCellTag, ICachingObjectServicePtr> GetCachingObjectServices()
    {
        auto guard = ReaderGuard(CachingObjectServicesLock_);
        return CachingObjectServices_;
    }

    void InitCachingObjectService(TCellId cellId)
    {
        YT_ASSERT_WRITER_SPINLOCK_AFFINITY(CachingObjectServicesLock_);

        auto cachingObjectService = CreateCachingObjectService(
            Config_->CachingObjectService,
            MasterCacheQueue_->GetInvoker(),
            CreateMasterChannelForCache(GetConnection(), cellId),
            ObjectServiceCache_,
            cellId,
            Logger(),
            ClusterNodeProfiler().WithPrefix("/caching_object_service"),
            NativeAuthenticator_);
        EmplaceOrCrash(CachingObjectServices_, CellTagFromId(cellId), cachingObjectService);
        if (MasterConnector_->IsRegisteredAtPrimaryMaster()) {
            RpcServer_->RegisterService(std::move(cachingObjectService));
        }
    }

    void InitProxyingChunkService(const NApi::NNative::TMasterConnectionConfigPtr& config)
    {
        auto service = CreateProxyingChunkService(
            config->CellId,
            Config_->ProxyingChunkService,
            config,
            Config_->ClusterConnection->Dynamic,
            Connection_->GetChannelFactory(),
            NativeAuthenticator_);
        RpcServer_->RegisterService(service);
        ProxyingChunkServices_[CellTagFromId(config->CellId)] = std::move(service);
    }

    void OnMasterCellDirectoryChanged(
        const TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs,
        const TSecondaryMasterConnectionConfigs& changedSecondaryMasterConfigs,
        const THashSet<TCellTag>& removedSecondaryMasterCellTags)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_LOG_ALERT_UNLESS(
            removedSecondaryMasterCellTags.empty(),
            "Some cells disappeared in received configuration of secondary masters (RemovedCellTags: %v)",
            removedSecondaryMasterCellTags);

        const auto& hiveCellDirectory = Connection_->GetCellDirectory();

        THashSet<TCellTag> newSecondaryMasterCellTags;
        THashSet<TCellTag> changedSecondaryMasterCellTags;
        newSecondaryMasterCellTags.reserve(newSecondaryMasterConfigs.size());
        changedSecondaryMasterCellTags.reserve(changedSecondaryMasterConfigs.size());

        auto addMasterCell = [&] (const auto& masterConfig) {
            InitCachingObjectService(masterConfig->CellId);
            InitProxyingChunkService(masterConfig);
            hiveCellDirectory->ReconfigureCell(masterConfig);
        };

        auto reconfigureMasterCell = [&] (const auto& masterConfig) {
            auto cellTag = CellTagFromId(masterConfig->CellId);
            // NB: It's necessary to reinitialize only ProxyingChunkServices_, since it uses the config completely,
            // while in CachingObjectServices_ only the CellId is used - it does not need to be reinitialized.
            RpcServer_->UnregisterService(ProxyingChunkServices_[cellTag]);
            InitProxyingChunkService(masterConfig);
        };

        {
            auto guard = WriterGuard(CachingObjectServicesLock_);
            for (const auto& [cellTag, masterConfig] : newSecondaryMasterConfigs) {
                addMasterCell(masterConfig);
                InsertOrCrash(newSecondaryMasterCellTags, cellTag);
            }
        }

        for (const auto& [cellTag, masterConfig] : changedSecondaryMasterConfigs) {
            reconfigureMasterCell(masterConfig);
            InsertOrCrash(changedSecondaryMasterCellTags, cellTag);
        }

        // For correct update of heartbeat reports to masters it is needed to update nodes state in the first place,
        // for this purpose there are two signals: the first triggers different node-flavors state update and the second performs change of heartbeats to masters.
        SecondaryMasterCellListChanged_.Fire(newSecondaryMasterConfigs);
        ReadyToUpdateHeartbeatStream_.Fire(newSecondaryMasterConfigs);

        {
            auto guard = WriterGuard(SecondaryMasterConnectionLock_);
            for (const auto& [cellTag, masterConfig] : newSecondaryMasterConfigs) {
                EmplaceOrCrash(SecondaryMasterConnectionConfigs_, cellTag, masterConfig);
            }
            for (const auto& [cellTag, masterConfig] : changedSecondaryMasterConfigs) {
                auto masterConfigIt = SecondaryMasterConnectionConfigs_.find(cellTag);
                YT_VERIFY(masterConfigIt != SecondaryMasterConnectionConfigs_.end());
                masterConfigIt->second = masterConfig;
            }
        }

        YT_LOG_INFO("Received new master cell cluster configuration "
            "(NewCellTags: %v, ChangedCellTags: %v, RemovedCellTags: %v)",
            newSecondaryMasterCellTags,
            changedSecondaryMasterCellTags,
            removedSecondaryMasterCellTags);
    }

    TSecondaryMasterConnectionConfigs GetSecondaryMasterConnectionConfigs() const
    {
        auto guard = ReaderGuard(SecondaryMasterConnectionLock_);
        return SecondaryMasterConnectionConfigs_;
    }

    i64 GetNetworkThrottlerLimit(const TClusterNodeDynamicConfigPtr& dynamicConfig, std::optional<i64> portoNetLimit) const
    {
        auto throttlerFreeBandwidthRatio = dynamicConfig
            ? dynamicConfig->ThrottlerFreeBandwidthRatio.value_or(Config_->ThrottlerFreeBandwidthRatio)
            : Config_->ThrottlerFreeBandwidthRatio;

        std::optional<i64> netLimit;

        if (dynamicConfig && dynamicConfig->UsePortoNetworkLimitInThrottler) {
            netLimit = portoNetLimit;
        }

        return netLimit.value_or(Config_->NetworkBandwidth) * (1. - throttlerFreeBandwidthRatio);
    }
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateNodeBootstrap(
    TClusterNodeBootstrapConfigPtr config,
    INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
{
    return New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
}

////////////////////////////////////////////////////////////////////////////////

TBootstrapBase::TBootstrapBase(IBootstrapBase* bootstrap)
    : Bootstrap_(bootstrap)
{
    // Cycles are fine for bootstrap.
    Bootstrap_->SubscribeMasterConnected(
        BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] (TNodeId nodeId) {
            MasterConnected_.Fire(nodeId);
        }));
    Bootstrap_->SubscribeMasterDisconnected(
        BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] {
            MasterDisconnected_.Fire();
        }));
    Bootstrap_->SubscribePopulateAlerts(
        BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] (std::vector<TError>* alerts) {
            PopulateAlerts_.Fire(alerts);
        }));
    Bootstrap_->SubscribeSecondaryMasterCellListChanged(
        BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] (const TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs) {
            SecondaryMasterCellListChanged_.Fire(newSecondaryMasterConfigs);
        }));
    Bootstrap_->SubscribeReadyToUpdateHeartbeatStream(
        BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] (const TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs) {
            ReadyToUpdateHeartbeatStream_.Fire(newSecondaryMasterConfigs);
        }));
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

const NConcurrency::IThroughputThrottlerPtr& TBootstrapBase::GetUserJobContainerCreationThrottler() const
{
    return Bootstrap_->GetUserJobContainerCreationThrottler();
}

const IThroughputThrottlerPtr& TBootstrapBase::GetAnnounceChunkReplicaRpsOutThrottler() const
{
    return Bootstrap_->GetAnnounceChunkReplicaRpsOutThrottler();
}

const TBufferedProducerPtr& TBootstrapBase::GetBufferedProducer() const
{
    return Bootstrap_->GetBufferedProducer();
}

const TClusterNodeBootstrapConfigPtr& TBootstrapBase::GetConfig() const
{
    return Bootstrap_->GetConfig();
}

const TClusterNodeDynamicConfigManagerPtr& TBootstrapBase::GetDynamicConfigManager() const
{
    return Bootstrap_->GetDynamicConfigManager();
}

const NCellarNode::TBundleDynamicConfigManagerPtr& TBootstrapBase::GetBundleDynamicConfigManager() const
{
    return Bootstrap_->GetBundleDynamicConfigManager();
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

const NNative::IClientPtr& TBootstrapBase::GetClient() const
{
    return Bootstrap_->GetClient();
}

const NNative::IConnectionPtr& TBootstrapBase::GetConnection() const
{
    return Bootstrap_->GetConnection();
}

IChannelPtr TBootstrapBase::GetMasterChannel(TCellTag cellTag)
{
    return Bootstrap_->GetMasterChannel(cellTag);
}

const IAuthenticatorPtr& TBootstrapBase::GetNativeAuthenticator() const
{
    return Bootstrap_->GetNativeAuthenticator();
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

std::vector<std::string> TBootstrapBase::GetMasterAddressesOrThrow(TCellTag cellTag) const
{
    return Bootstrap_->GetMasterAddressesOrThrow(cellTag);
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

std::string TBootstrapBase::GetLocalHostName() const
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

TNetworkPreferenceList TBootstrapBase::GetNetworks() const
{
    return Bootstrap_->GetNetworks();
}

TNetworkPreferenceList TBootstrapBase::GetLocalNetworks() const
{
    return Bootstrap_->GetLocalNetworks();
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

const INodeMemoryTrackerPtr& TBootstrapBase::GetNodeMemoryUsageTracker() const
{
    return Bootstrap_->GetNodeMemoryUsageTracker();
}

const IMemoryUsageTrackerPtr& TBootstrapBase::GetRpcMemoryUsageTracker() const
{
    return Bootstrap_->GetRpcMemoryUsageTracker();
}

const IMemoryUsageTrackerPtr& TBootstrapBase::GetReadBlockMemoryUsageTracker() const
{
    return Bootstrap_->GetReadBlockMemoryUsageTracker();
}

const IMemoryUsageTrackerPtr& TBootstrapBase::GetSystemJobsMemoryUsageTracker() const
{
    return Bootstrap_->GetSystemJobsMemoryUsageTracker();
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

const NJobAgent::TJobResourceManagerPtr& TBootstrapBase::GetJobResourceManager() const
{
    return Bootstrap_->GetJobResourceManager();
}

const TRestartManagerPtr& TBootstrapBase::GetRestartManager() const
{
    return Bootstrap_->GetRestartManager();
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

const NClusterNode::IBootstrap* TBootstrapBase::GetClusterNodeBootstrap() const
{
    return Bootstrap_->GetClusterNodeBootstrap();
}

NTabletNode::IBootstrap* TBootstrapBase::GetTabletNodeBootstrap() const
{
    return Bootstrap_->GetTabletNodeBootstrap();
}

bool TBootstrapBase::NeedDataNodeBootstrap() const
{
    return Bootstrap_->NeedDataNodeBootstrap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
