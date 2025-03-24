#include "bootstrap.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/chunk_detail.h>
#include <yt/yt/server/node/data_node/chunk_store.h>
#include <yt/yt/server/node/data_node/job_controller.h>
#include <yt/yt/server/node/data_node/master_connector.h>
#include <yt/yt/server/node/data_node/network_statistics.h>
#include <yt/yt/server/node/data_node/session_manager.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;
using namespace NClusterNode;
using namespace NLogging;
using namespace NDataNode;

class TBootstrap
    : public NDataNode::IBootstrap
{
public:
    TBootstrap(
        IInvokerPtr invoker,
        TClusterNodeBootstrapConfigPtr config,
        TLogger logger,
        NProfiling::TProfiler profiler)
        : Invoker_(std::move(invoker))
        , Config_(std::move(config))
        , Logger(std::move(logger))
        , Profiler_(std::move(profiler))
    { }

    DEFINE_SIGNAL_OVERRIDE(void(NNodeTrackerClient::TNodeId nodeId), MasterConnected);
    DEFINE_SIGNAL_OVERRIDE(void(), MasterDisconnected);
    DEFINE_SIGNAL_OVERRIDE(void(std::vector<TError>* alerts), PopulateAlerts);
    DEFINE_SIGNAL_OVERRIDE(void(const NCellMasterClient::TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs), SecondaryMasterCellListChanged);
    DEFINE_SIGNAL_OVERRIDE(void(const NCellMasterClient::TSecondaryMasterConnectionConfigs& newSecondaryMasterConfigs), ReadyToUpdateHeartbeatStream);

    const IInvokerPtr& GetStorageLightInvoker() const override
    {
        return Invoker_;
    }

    void Initialize() override
    {
        InThrottler_ = GetUnlimitedThrottler();
        OutThrottler_ = GetUnlimitedThrottler();
        StorageHeavyInvoker_ = CreatePrioritizedInvoker(Invoker_);

        DynamicConfigManager_ = New<TClusterNodeDynamicConfigManager>(New<TClusterNodeDynamicConfig>());

        NodeMemoryUsageTracker_ = CreateNodeMemoryTracker(
            Config_->ResourceLimits->TotalMemory,
            /*limits*/ {},
            Logger,
            Profiler_.WithPrefix("/memory_usage"));

        FairShareHierarchicalScheduler_ = CreateFairShareHierarchicalScheduler<TString>(
            New<TFairShareHierarchicalSchedulerDynamicConfig>(),
            {});
        ChunkStore_ = New<TChunkStore>(
            Config_->DataNode,
            GetDynamicConfigManager(),
            Invoker_,
            TChunkContext::Create(this),
            CreateChunkStoreHost(this));

        SessionManager_ = New<TSessionManager>(Config_->DataNode, this);

        auto future = BIND(&TBootstrap::DoInitialize, MakeStrong(this))
            .AsyncVia(GetControlInvoker())
            .Run();
        WaitFor(future).ThrowOnError();
    }

    void DoInitialize()
    {
        ChunkStore_->Initialize();

        SessionManager_->Initialize();
    }

    void Run() override
    {
        return;
    }

    const NYT::NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const override
    {
        return NativeAuthenticator_;
    }

    const NYT::NDataNode::TChunkStorePtr& GetChunkStore() const override
    {
        return ChunkStore_;
    }

    const NYT::NDataNode::IAllyReplicaManagerPtr& GetAllyReplicaManager() const override
    {
        return AllyReplicaManager_;
    }

    const NYT::NDataNode::TLocationManagerPtr& GetLocationManager() const override
    {
        return LocationManager_;
    }

    const NYT::NDataNode::TSessionManagerPtr& GetSessionManager() const override
    {
        return SessionManager_;
    }

    const NYT::NDataNode::IJobControllerPtr& GetJobController() const override
    {
        return JobController_;
    }

    const INodeMemoryTrackerPtr& GetNodeMemoryUsageTracker() const override
    {
        return NodeMemoryUsageTracker_;
    }

    const NConcurrency::IThroughputThrottlerPtr& GetDefaultInThrottler() const override
    {
        return DefaultInThrottler_;
    }

    const NYT::NClusterNode::TNodeResourceManagerPtr& GetNodeResourceManager() const override
    {
        return NodeResourceManager_;
    }

    const NConcurrency::IThroughputThrottlerPtr& GetDefaultOutThrottler() const override
    {
        return DefaultOutThrottler_;
    }

    const NConcurrency::IThroughputThrottlerPtr& GetReadRpsOutThrottler() const override
    {
        return ReadRpsOutThrottler_;
    }

    const NConcurrency::IThroughputThrottlerPtr& GetAnnounceChunkReplicaRpsOutThrottler() const override
    {
        return AnnounceChunkReplicaRpsOutThrottler_;
    }

    const NConcurrency::IThroughputThrottlerPtr& GetUserJobContainerCreationThrottler() const override
    {
        return UserJobContainerCreationThrottler_;
    }

    const NProfiling::TBufferedProducerPtr& GetBufferedProducer() const override
    {
        return BufferedProducer_;
    }

    const NClusterNode::TClusterNodeBootstrapConfigPtr& GetConfig() const override
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
        // ControllerInvoker_;
        return Invoker_;
    }

    const IInvokerPtr& GetJobInvoker() const override
    {
        return JobInvoker_;
    }

    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() const override
    {
        return NodeDirectory_;
    }

    NNodeTrackerClient::TNetworkPreferenceList GetNetworks() const override
    {
        return Networks_;
    }

    NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks() const  override
    {
        return LocalNetworks_;
    }

    const NHttp::IServerPtr& GetHttpServer() const override
    {
        return HttpServer_;
    }

    const NRpc::IServerPtr& GetRpcServer() const override
    {
        return RpcServer_;
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

    const NChunkClient::IBlockCachePtr& GetBlockCache() const override
    {
        return BlockCache_;
    }

    const NDataNode::IChunkMetaManagerPtr& GetChunkMetaManager() const override
    {
        return ChunkMetaManager_;
    }

    const NTabletNode::IVersionedChunkMetaManagerPtr& GetVersionedChunkMetaManager() const override
    {
        return VersionedChunkMetaManager_;
    }

    const NDataNode::TChunkReaderSweeperPtr& GetChunkReaderSweeper() const override
    {
        return ChunkReaderSweeper_;
    }

    const NYTree::IMapNodePtr& GetOrchidRoot() const override
    {
        return OrchidRoot_;
    }

    const IInvokerPtr& GetMasterConnectionInvoker() const override
    {
        return MasterConnectionInvoker_;
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

    NRpc::IChannelPtr GetMasterChannel(NObjectClient::TCellTag) override
    {
        return nullptr;
    }

    NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() const override
    {
        return {};
    }

    NObjectClient::TCellId GetCellId() const override
    {
        return {};
    }

    NObjectClient::TCellId GetCellId(NObjectClient::TCellTag) const override
    {
        return {};
    }

    std::vector<std::string> GetMasterAddressesOrThrow(NObjectClient::TCellTag) const override
    {
        return {};
    }

    void ResetAndRegisterAtMaster() override
    {
        return;
    }

    bool IsConnected() const override
    {
        return true;
    }

    NNodeTrackerClient::TNodeId GetNodeId() const override
    {
        return {};
    }

    std::string GetLocalHostName() const override
    {
        return {};
    }

    NYT::NClusterNode::TMasterEpoch GetMasterEpoch() const override
    {
        return {};
    }

    bool IsReadOnly() const override
    {
        return false;
    }

    bool IsDecommissioned() const override
    {
        return false;
    }

    NDataNode::TNetworkStatistics& GetNetworkStatistics() const override
    {
        return NetworkStatistics_;
    }

    const NDataNode::IChunkRegistryPtr& GetChunkRegistry() const override
    {
        return ChunkRegistry_;
    }

    const NDataNode::IBlobReaderCachePtr& GetBlobReaderCache() const override
    {
        return BlobReaderCache_;
    }

    const NJobAgent::TJobResourceManagerPtr& GetJobResourceManager() const override
    {
        return JobResourceManager_;
    }

    const NServer::TRestartManagerPtr& GetRestartManager() const override
    {
        return RestartManager_;
    }

    NJobProxy::EJobEnvironmentType GetJobEnvironmentType() const override
    {
        return {};
    }

    const NIO::IIOTrackerPtr& GetIOTracker() const override
    {
        return IOTracker_;
    }

    const THashSet<NNodeTrackerClient::ENodeFlavor>& GetFlavors() const override
    {
        return Flavors_;
    }

    bool IsDataNode() const override
    {
        return false;
    }

    bool IsExecNode() const override
    {
        return false;
    }

    bool IsCellarNode() const override
    {
        return false;
    }

    bool IsTabletNode() const override
    {
        return false;
    }

    bool IsChaosNode() const override
    {
        return false;
    }

    NCellarNode::IBootstrap* GetCellarNodeBootstrap() const override
    {
        return nullptr;
    }

    NDataNode::IBootstrap* GetDataNodeBootstrap() const override
    {
        return nullptr;
    }

    NExecNode::IBootstrap* GetExecNodeBootstrap() const override
    {
        return nullptr;
    }

    NChaosNode::IBootstrap* GetChaosNodeBootstrap() const override
    {
        return nullptr;
    }

    NTabletNode::IBootstrap* GetTabletNodeBootstrap() const override
    {
        return nullptr;
    }

    const NClusterNode::IBootstrap* GetClusterNodeBootstrap() const override
    {
        return nullptr;
    }

    const TFairShareHierarchicalSchedulerPtr<TString>& GetFairShareHierarchicalScheduler() const override
    {
        return FairShareHierarchicalScheduler_;
    }

    bool NeedDataNodeBootstrap() const override
    {
        return false;
    }

    const NYT::NDataNode::IMasterConnectorPtr& GetMasterConnector() const override
    {
        return MasterConnector_;
    }

    const NYT::NDataNode::TMediumDirectoryManagerPtr& GetMediumDirectoryManager() const override
    {
        return MediumDirectoryManager_;
    }

    const NYT::NDataNode::TMediumUpdaterPtr& GetMediumUpdater() const override
    {
        return MediumUpdater_;
    }

    const NConcurrency::IThroughputThrottlerPtr& GetThrottler(NYT::NDataNode::EDataNodeThrottlerKind) const override
    {
        return Throttler_;
    }

    const NConcurrency::IThroughputThrottlerPtr& GetInThrottler(const TWorkloadDescriptor&) const override
    {
        return InThrottler_;
    }

    const NConcurrency::IThroughputThrottlerPtr& GetOutThrottler(const TWorkloadDescriptor&) const override
    {
        return OutThrottler_;
    }

    const NYT::NDataNode::IJournalDispatcherPtr& GetJournalDispatcher() const override
    {
        return JournalDispatcher_;
    }

    const IInvokerPtr& GetStorageLookupInvoker() const override
    {
        return StorageLookupInvoker_;
    }

    const IInvokerPtr& GetMasterJobInvoker() const override
    {
        return MasterJobInvoker_;
    }

    const NYT::NDataNode::TP2PBlockCachePtr& GetP2PBlockCache() const override
    {
        return P2PBlockCache_;
    }

    const NYT::NDataNode::TP2PSnooperPtr& GetP2PSnooper() const override
    {
        return P2PSnooper_;
    }

    const NYT::NDataNode::TTableSchemaCachePtr& GetTableSchemaCache() const override
    {
        return TableSchemaCache_;
    }

    const NQueryClient::IRowComparerProviderPtr& GetRowComparerProvider() const override
    {
        return RowComparerProvider_;
    }

    const NYT::NDataNode::IIOThroughputMeterPtr& GetIOThroughputMeter() const override
    {
        return IOThroughputMeter_;
    }

    const NYT::NDataNode::TLocationHealthCheckerPtr& GetLocationHealthChecker() const override
    {
        return LocationHealthChecker_;
    }

    void SetLocationUuidsRequired(bool) override
    {
        return;
    }

    void SetPerLocationFullHeartbeatsEnabled(bool) override
    {
        return;
    }

private:
    NYT::IInvokerPtr Invoker_;

    // Null fields.
    NRpc::IAuthenticatorPtr NativeAuthenticator_;
    NDataNode::TChunkStorePtr ChunkStore_;
    NDataNode::IAllyReplicaManagerPtr AllyReplicaManager_;
    NDataNode::TLocationManagerPtr LocationManager_;
    NDataNode::TSessionManagerPtr SessionManager_;
    NDataNode::IJobControllerPtr JobController_;
    INodeMemoryTrackerPtr NodeMemoryUsageTracker_;
    TFairShareHierarchicalSchedulerPtr<TString> FairShareHierarchicalScheduler_;
    NConcurrency::IThroughputThrottlerPtr DefaultInThrottler_;
    NClusterNode::TNodeResourceManagerPtr NodeResourceManager_;
    NConcurrency::IThroughputThrottlerPtr DefaultOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr ReadRpsOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr AnnounceChunkReplicaRpsOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr UserJobContainerCreationThrottler_;
    NProfiling::TBufferedProducerPtr BufferedProducer_;
    NClusterNode::TClusterNodeBootstrapConfigPtr Config_;
    NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    NCellarNode::TBundleDynamicConfigManagerPtr BundleDynamicConfigManager_;
    IInvokerPtr ControlInvoker_;
    IInvokerPtr JobInvoker_;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    NNodeTrackerClient::TNetworkPreferenceList Networks_;
    NNodeTrackerClient::TNetworkPreferenceList LocalNetworks_;
    NHttp::IServerPtr HttpServer_;
    NRpc::IServerPtr RpcServer_;
    IMemoryUsageTrackerPtr RpcMemoryUsageTracker_;
    IMemoryUsageTrackerPtr ReadBlockMemoryUsageTracker_;
    IMemoryUsageTrackerPtr SystemJobsMemoryUsageTracker_;
    NChunkClient::IBlockCachePtr BlockCache_;
    NDataNode::IChunkMetaManagerPtr ChunkMetaManager_;
    NTabletNode::IVersionedChunkMetaManagerPtr VersionedChunkMetaManager_;
    NDataNode::TChunkReaderSweeperPtr ChunkReaderSweeper_;
    NYTree::IMapNodePtr OrchidRoot_;
    IInvokerPtr MasterConnectionInvoker_;
    IPrioritizedInvokerPtr StorageHeavyInvoker_;
    NApi::NNative::IClientPtr Client_;
    NApi::NNative::IConnectionPtr Connection_;
    mutable NDataNode::TNetworkStatistics NetworkStatistics_ = NDataNode::TNetworkStatistics(nullptr);
    NDataNode::IChunkRegistryPtr ChunkRegistry_;
    NDataNode::IBlobReaderCachePtr BlobReaderCache_;
    NJobAgent::TJobResourceManagerPtr JobResourceManager_;
    NServer::TRestartManagerPtr RestartManager_;
    NIO::IIOTrackerPtr IOTracker_;
    THashSet<NNodeTrackerClient::ENodeFlavor> Flavors_;
    NYT::NDataNode::IMasterConnectorPtr MasterConnector_;
    NYT::NDataNode::TMediumDirectoryManagerPtr MediumDirectoryManager_;
    NYT::NDataNode::TMediumUpdaterPtr MediumUpdater_;
    NConcurrency::IThroughputThrottlerPtr Throttler_;
    NConcurrency::IThroughputThrottlerPtr InThrottler_;
    NConcurrency::IThroughputThrottlerPtr OutThrottler_;
    NYT::NDataNode::IJournalDispatcherPtr JournalDispatcher_;
    IInvokerPtr StorageLookupInvoker_;
    IInvokerPtr MasterJobInvoker_;
    NYT::NDataNode::TP2PBlockCachePtr P2PBlockCache_;
    NYT::NDataNode::TP2PSnooperPtr P2PSnooper_;
    NYT::NDataNode::TTableSchemaCachePtr TableSchemaCache_;
    NQueryClient::IRowComparerProviderPtr RowComparerProvider_;
    NYT::NDataNode::IIOThroughputMeterPtr IOThroughputMeter_;
    NYT::NDataNode::TLocationHealthCheckerPtr LocationHealthChecker_;

    TLogger Logger;
    NProfiling::TProfiler Profiler_;
};

////////////////////////////////////////////////////////////////////////////////

NDataNode::IBootstrapPtr CreateBootstrap(
    IInvokerPtr invoker,
    TClusterNodeBootstrapConfigPtr config,
    TLogger logger,
    NProfiling::TProfiler profiler)
{
    return New<TBootstrap>(
        std::move(invoker),
        std::move(config),
        std::move(logger),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////
