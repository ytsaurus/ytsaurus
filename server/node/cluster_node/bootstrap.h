#pragma once

#include "public.h"

#include <yt/server/node/exec_agent/public.h>

#include <yt/server/node/data_node/public.h>

#include <yt/server/node/job_agent/public.h>

#include <yt/server/node/query_agent/public.h>

#include <yt/server/node/tablet_node/public.h>

#include <yt/server/lib/containers/public.h>

#include <yt/server/lib/job_proxy/public.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/misc/public.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/query_client/public.h>

#include <yt/ytlib/monitoring/public.h>

#include <yt/core/bus/public.h>

#include <yt/core/http/public.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/two_level_fair_share_thread_pool.h>

#include <yt/core/net/address.h>

#include <yt/core/rpc/public.h>

#include <yt/core/ytree/public.h>

#include <yt/core/misc/public.h>
#include <yt/core/misc/lazy_ptr.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TClusterNodeConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    const TClusterNodeConfigPtr& GetConfig() const;
    const IInvokerPtr& GetControlInvoker() const;
    const IInvokerPtr& GetJobInvoker() const;
    IInvokerPtr GetQueryPoolInvoker(
        const TString& poolName,
        double weight,
        const NConcurrency::TFairShareThreadPoolTag& tag) const;
    const IInvokerPtr& GetTabletLookupPoolInvoker() const;
    const IInvokerPtr& GetTableReplicatorPoolInvoker() const;
    const IInvokerPtr& GetTransactionTrackerInvoker() const;
    const IPrioritizedInvokerPtr& GetStorageHeavyInvoker() const;
    const IInvokerPtr& GetStorageLightInvoker() const;
    IInvokerPtr GetStorageLookupInvoker() const;
    const IInvokerPtr& GetJobThrottlerInvoker() const;
    const NApi::NNative::IClientPtr& GetMasterClient() const;
    const NApi::NNative::IConnectionPtr& GetMasterConnection() const;
    const NRpc::IServerPtr& GetRpcServer() const;
    const NYTree::IMapNodePtr& GetOrchidRoot() const;
    const NJobAgent::TJobControllerPtr& GetJobController() const;
    const NJobAgent::TJobReporterPtr& GetJobReporter() const;
    const NTabletNode::TSlotManagerPtr& GetTabletSlotManager() const;
    const NTabletNode::TSecurityManagerPtr& GetSecurityManager() const;
    const NTabletNode::IInMemoryManagerPtr& GetInMemoryManager() const;
    const NTabletNode::TVersionedChunkMetaManagerPtr& GetVersionedChunkMetaManager() const;
    const NExecAgent::TSlotManagerPtr& GetExecSlotManager() const;
    const NJobAgent::TGpuManagerPtr& GetGpuManager() const;
    const TNodeMemoryTrackerPtr& GetMemoryUsageTracker() const;
    const NDataNode::TChunkStorePtr& GetChunkStore() const;
    const NDataNode::TChunkCachePtr& GetChunkCache() const;
    const NDataNode::TChunkRegistryPtr& GetChunkRegistry() const;
    const NDataNode::TSessionManagerPtr& GetSessionManager() const;
    const NDataNode::TChunkMetaManagerPtr& GetChunkMetaManager() const;
    const NDataNode::TChunkBlockManagerPtr& GetChunkBlockManager() const;
    NDataNode::TNetworkStatistics& GetNetworkStatistics() const;
    const NChunkClient::IBlockCachePtr& GetBlockCache() const;
    const NTableClient::TBlockMetaCachePtr& GetBlockMetaCache() const;
    const NDataNode::TPeerBlockDistributorPtr& GetPeerBlockDistributor() const;
    const NDataNode::TPeerBlockTablePtr& GetPeerBlockTable() const;
    const NDataNode::TPeerBlockUpdaterPtr& GetPeerBlockUpdater() const;
    const NDataNode::TBlobReaderCachePtr& GetBlobReaderCache() const;
    const NDataNode::TTableSchemaCachePtr& GetTableSchemaCache() const;
    const NDataNode::TJournalDispatcherPtr& GetJournalDispatcher() const;
    const NDataNode::TMasterConnectorPtr& GetMasterConnector() const;
    const NQueryClient::TColumnEvaluatorCachePtr& GetColumnEvaluatorCache() const;
    const NQueryAgent::IQuerySubexecutorPtr& GetQueryExecutor() const;
    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() const;
    const TDynamicConfigManagerPtr& GetDynamicConfigManager() const;
    const TNodeResourceManagerPtr& GetNodeResourceManager() const;
#ifdef __linux__
    const NContainers::TInstanceLimitsTrackerPtr& GetInstanceLimitsTracker() const;
#endif

    const NConcurrency::IThroughputThrottlerPtr& GetReplicationInThrottler() const;
    const NConcurrency::IThroughputThrottlerPtr& GetReplicationOutThrottler() const;
    const NConcurrency::IThroughputThrottlerPtr& GetRepairInThrottler() const;
    const NConcurrency::IThroughputThrottlerPtr& GetRepairOutThrottler() const;
    const NConcurrency::IThroughputThrottlerPtr& GetArtifactCacheInThrottler() const;
    const NConcurrency::IThroughputThrottlerPtr& GetArtifactCacheOutThrottler() const;
    const NConcurrency::IThroughputThrottlerPtr& GetSkynetOutThrottler() const;

    const NConcurrency::IThroughputThrottlerPtr& GetInThrottler(const TWorkloadDescriptor& descriptor) const;
    const NConcurrency::IThroughputThrottlerPtr& GetOutThrottler(const TWorkloadDescriptor& descriptor) const;

    const NConcurrency::IThroughputThrottlerPtr& GetTabletNodeInThrottler(EWorkloadCategory category) const;
    const NConcurrency::IThroughputThrottlerPtr& GetTabletNodeOutThrottler(EWorkloadCategory category) const;

    const NConcurrency::IThroughputThrottlerPtr& GetReadRpsOutThrottler() const;

    NObjectClient::TCellId GetCellId() const;
    NObjectClient::TCellId GetCellId(NObjectClient::TCellTag cellTag) const;
    NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks();
    std::optional<TString> GetDefaultNetworkName();
    NExecAgent::EJobEnvironmentType GetEnvironmentType() const;
    const std::vector<NNet::TIP6Address>& GetResolvedNodeAddresses() const;

    NJobProxy::TJobProxyConfigPtr BuildJobProxyConfig() const;

    NTransactionClient::TTimestamp GetLatestTimestamp() const;

    void Initialize();
    void Run();
    void ValidateSnapshot(const TString& fileName);

    bool IsReadOnly() const;

private:
    const TClusterNodeConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    NConcurrency::TActionQueuePtr ControlActionQueue_;
    NConcurrency::TActionQueuePtr JobActionQueue_;
    TLazyIntrusivePtr<NConcurrency::ITwoLevelFairShareThreadPool> QueryThreadPool_;
    NConcurrency::TThreadPoolPtr TabletLookupThreadPool_;
    NConcurrency::TThreadPoolPtr TableReplicatorThreadPool_;
    NConcurrency::TActionQueuePtr TransactionTrackerQueue_;
    NConcurrency::TThreadPoolPtr StorageHeavyThreadPool_;
    IPrioritizedInvokerPtr StorageHeavyInvoker_;
    NConcurrency::TThreadPoolPtr StorageLightThreadPool_;
    NConcurrency::IFairShareThreadPoolPtr StorageLookupThreadPool_;
    NConcurrency::TActionQueuePtr MasterCacheQueue_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NBus::IBusServerPtr BusServer_;
    NApi::NNative::IConnectionPtr MasterConnection_;
    NApi::NNative::IClientPtr MasterClient_;
    NRpc::IServerPtr RpcServer_;
    std::vector<NRpc::IServicePtr> MasterCacheServices_;
    NHttp::IServerPtr HttpServer_;
    NHttp::IServerPtr SkynetHttpServer_;
    NYTree::IMapNodePtr OrchidRoot_;
    NJobAgent::TJobControllerPtr JobController_;
    NJobAgent::TJobReporterPtr JobReporter_;
    NExecAgent::TSlotManagerPtr ExecSlotManager_;
    NJobAgent::TGpuManagerPtr GpuManager_;
    NJobProxy::TJobProxyConfigPtr JobProxyConfigTemplate_;
    NNodeTrackerClient::TNodeMemoryTrackerPtr MemoryUsageTracker_;
    NExecAgent::TSchedulerConnectorPtr SchedulerConnector_;
    NDataNode::TChunkStorePtr ChunkStore_;
    NDataNode::TChunkCachePtr ChunkCache_;
    NDataNode::TChunkRegistryPtr ChunkRegistry_;
    NDataNode::TSessionManagerPtr SessionManager_;
    NDataNode::TChunkMetaManagerPtr ChunkMetaManager_;
    NDataNode::TChunkBlockManagerPtr ChunkBlockManager_;
    std::unique_ptr<NDataNode::TNetworkStatistics> NetworkStatistics_;
    NChunkClient::IBlockCachePtr BlockCache_;
    NTableClient::TBlockMetaCachePtr BlockMetaCache_;
    NDataNode::TPeerBlockTablePtr PeerBlockTable_;
    NDataNode::TPeerBlockUpdaterPtr PeerBlockUpdater_;
    NDataNode::TPeerBlockDistributorPtr PeerBlockDistributor_;
    NDataNode::TBlobReaderCachePtr BlobReaderCache_;
    NDataNode::TTableSchemaCachePtr TableSchemaCache_;
    NDataNode::TJournalDispatcherPtr JournalDispatcher_;
    NDataNode::TMasterConnectorPtr MasterConnector_;
    ICoreDumperPtr CoreDumper_;
    TDynamicConfigManagerPtr DynamicConfigManager_;

    NConcurrency::IThroughputThrottlerPtr TotalInThrottler_;
    NConcurrency::IThroughputThrottlerPtr TotalOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr ReplicationInThrottler_;
    NConcurrency::IThroughputThrottlerPtr ReplicationOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr RepairInThrottler_;
    NConcurrency::IThroughputThrottlerPtr RepairOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr ArtifactCacheInThrottler_;
    NConcurrency::IThroughputThrottlerPtr ArtifactCacheOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr SkynetOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr DataNodeTabletCompactionAndPartitioningInThrottler_;
    NConcurrency::IThroughputThrottlerPtr DataNodeTabletCompactionAndPartitioningOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr DataNodeTabletStoreFlushInThrottler_;
    NConcurrency::IThroughputThrottlerPtr DataNodeTabletLoggingInThrottler_;
    NConcurrency::IThroughputThrottlerPtr DataNodeTabletPreloadOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr DataNodeTabletSnapshotInThrottler_;
    NConcurrency::IThroughputThrottlerPtr DataNodeTabletRecoveryOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr DataNodeTabletReplicationOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletNodeCompactionAndPartitioningInThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletNodeCompactionAndPartitioningOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletNodeStoreFlushOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletNodePreloadInThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletNodeTabletReplicationInThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletNodeTabletReplicationOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr TabletNodeDynamicStoreReadOutThrottler_;
    NConcurrency::IThroughputThrottlerPtr ReadRpsOutThrottler_;

    NTabletNode::TSlotManagerPtr TabletSlotManager_;
    NTabletNode::TSecurityManagerPtr SecurityManager_;
    NTabletNode::IInMemoryManagerPtr InMemoryManager_;
    NTabletNode::TVersionedChunkMetaManagerPtr VersionedChunkMetaManager_;

    NQueryClient::TColumnEvaluatorCachePtr ColumnEvaluatorCache_;
    NQueryAgent::IQuerySubexecutorPtr QueryExecutor_;

#ifdef __linux__
    NContainers::TInstanceLimitsTrackerPtr InstanceLimitsTracker_;
#endif

    TNodeResourceManagerPtr NodeResourceManager_;

    std::vector<NNet::TIP6Address> ResolvedNodeAddresses_;

    void DoInitialize();
    void DoRun();
    void DoValidateConfig();
    void DoValidateSnapshot(const TString& fileName);
    void PopulateAlerts(std::vector<TError>* alerts);

    void OnMasterConnected();
    void OnMasterDisconnected();

    void OnDynamicConfigUpdated(const TClusterNodeDynamicConfigPtr& newConfig);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
