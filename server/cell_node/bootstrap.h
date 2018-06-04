#pragma once

#include "public.h"

#include <yt/server/chunk_server/public.h>

#include <yt/server/exec_agent/public.h>

#include <yt/server/data_node/public.h>

#include <yt/server/job_agent/public.h>

#include <yt/server/job_proxy/public.h>

#include <yt/server/tablet_node/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/misc/public.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/query_client/public.h>

#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/monitoring/public.h>

#include <yt/core/bus/public.h>

#include <yt/core/http/public.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/fair_share_thread_pool.h>

#include <yt/core/rpc/public.h>

#include <yt/core/ytree/public.h>

#include <yt/core/misc/public.h>
#include <yt/core/misc/lazy_ptr.h>


namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TCellNodeConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    const TCellNodeConfigPtr& GetConfig() const;
    const IInvokerPtr& GetControlInvoker() const;
    IInvokerPtr GetQueryPoolInvoker(const NConcurrency::TFairShareThreadPoolTag& tag) const;
    const IInvokerPtr& GetLookupPoolInvoker() const;
    const IInvokerPtr& GetTableReplicatorPoolInvoker() const;
    const IInvokerPtr& GetTransactionTrackerInvoker() const;
    const NApi::INativeClientPtr& GetMasterClient() const;
    const NApi::INativeConnectionPtr& GetMasterConnection() const;
    const NRpc::IServerPtr& GetRpcServer() const;
    const NYTree::IMapNodePtr& GetOrchidRoot() const;
    const NJobAgent::TJobControllerPtr& GetJobController() const;
    const NJobAgent::TStatisticsReporterPtr& GetStatisticsReporter() const;
    const NTabletNode::TSlotManagerPtr& GetTabletSlotManager() const;
    const NTabletNode::TSecurityManagerPtr& GetSecurityManager() const;
    const NTabletNode::TInMemoryManagerPtr& GetInMemoryManager() const;
    const NTabletNode::TVersionedChunkMetaManagerPtr& GetVersionedChunkMetaManager() const;
    const NExecAgent::TSlotManagerPtr& GetExecSlotManager() const;
    TNodeMemoryTracker* GetMemoryUsageTracker() const;
    const NDataNode::TChunkStorePtr& GetChunkStore() const;
    const NDataNode::TChunkCachePtr& GetChunkCache() const;
    const NDataNode::TChunkRegistryPtr& GetChunkRegistry() const;
    const NDataNode::TSessionManagerPtr& GetSessionManager() const;
    const NDataNode::TChunkMetaManagerPtr& GetChunkMetaManager() const;
    const NDataNode::TChunkBlockManagerPtr& GetChunkBlockManager() const;
    const NDataNode::TNetworkStatisticsPtr& GetNetworkStatistics() const;
    const NChunkClient::IBlockCachePtr& GetBlockCache() const;
    const NTableClient::TBlockMetaCachePtr& GetBlockMetaCache() const;
    const NDataNode::TPeerBlockDistributorPtr& GetPeerBlockDistributor() const;
    const NDataNode::TPeerBlockTablePtr& GetPeerBlockTable() const;
    const NDataNode::TPeerBlockUpdaterPtr& GetPeerBlockUpdater() const;
    const NDataNode::TBlobReaderCachePtr& GetBlobReaderCache() const;
    const NDataNode::TJournalDispatcherPtr& GetJournalDispatcher() const;
    const NDataNode::TMasterConnectorPtr& GetMasterConnector() const;
    const NQueryClient::TColumnEvaluatorCachePtr& GetColumnEvaluatorCache() const;
    const NQueryClient::ISubexecutorPtr& GetQueryExecutor() const;
    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() const;

    const NConcurrency::IThroughputThrottlerPtr& GetReplicationInThrottler() const;
    const NConcurrency::IThroughputThrottlerPtr& GetReplicationOutThrottler() const;
    const NConcurrency::IThroughputThrottlerPtr& GetRepairInThrottler() const;
    const NConcurrency::IThroughputThrottlerPtr& GetRepairOutThrottler() const;
    const NConcurrency::IThroughputThrottlerPtr& GetArtifactCacheInThrottler() const;
    const NConcurrency::IThroughputThrottlerPtr& GetArtifactCacheOutThrottler() const;
    const NConcurrency::IThroughputThrottlerPtr& GetSkynetOutThrottler() const;

    const NConcurrency::IThroughputThrottlerPtr& GetInThrottler(const TWorkloadDescriptor& descriptor) const;
    const NConcurrency::IThroughputThrottlerPtr& GetOutThrottler(const TWorkloadDescriptor& descriptor) const;

    const NObjectClient::TCellId& GetCellId() const;
    NObjectClient::TCellId GetCellId(NObjectClient::TCellTag cellTag) const;
    NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks();
    TNullable<TString> GetDefaultNetworkName();

    NJobProxy::TJobProxyConfigPtr BuildJobProxyConfig() const;

    NTransactionClient::TTimestamp GetLatestTimestamp() const;

    void Run();

private:
    const TCellNodeConfigPtr Config;
    const NYTree::INodePtr ConfigNode;

    NConcurrency::TActionQueuePtr ControlQueue;
    TLazyIntrusivePtr<NConcurrency::IFairShareThreadPool> QueryThreadPool;
    NConcurrency::TThreadPoolPtr LookupThreadPool;
    NConcurrency::TThreadPoolPtr TableReplicatorThreadPool;
    NConcurrency::TActionQueuePtr TransactionTrackerQueue;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    std::unique_ptr<NLFAlloc::TLFAllocProfiler> LFAllocProfiler_;
    NBus::IBusServerPtr BusServer;
    NApi::INativeConnectionPtr MasterConnection;
    NApi::INativeClientPtr MasterClient;
    NHiveClient::TCellDirectorySynchronizerPtr CellDirectorySynchronizer;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;
    NNodeTrackerClient::TNodeDirectorySynchronizerPtr NodeDirectorySynchronizer;
    NRpc::IServerPtr RpcServer;
    NRpc::IServicePtr MasterCacheService;
    NHttp::IServerPtr HttpServer;
    NHttp::IServerPtr SkynetHttpServer;
    NYTree::IMapNodePtr OrchidRoot;
    NJobAgent::TJobControllerPtr JobController;
    NJobAgent::TStatisticsReporterPtr StatisticsReporter;
    NExecAgent::TSlotManagerPtr ExecSlotManager;
    NJobProxy::TJobProxyConfigPtr JobProxyConfigTemplate;
    std::unique_ptr<TMemoryUsageTracker<NNodeTrackerClient::EMemoryCategory>> MemoryUsageTracker;
    NExecAgent::TSchedulerConnectorPtr SchedulerConnector;
    NDataNode::TChunkStorePtr ChunkStore;
    NDataNode::TChunkCachePtr ChunkCache;
    NDataNode::TChunkRegistryPtr ChunkRegistry;
    NDataNode::TSessionManagerPtr SessionManager;
    NDataNode::TChunkMetaManagerPtr ChunkMetaManager;
    NDataNode::TChunkBlockManagerPtr ChunkBlockManager;
    NDataNode::TNetworkStatisticsPtr NetworkStatistics;
    NChunkClient::IBlockCachePtr BlockCache;
    NTableClient::TBlockMetaCachePtr BlockMetaCache;
    NDataNode::TPeerBlockTablePtr PeerBlockTable;
    NDataNode::TPeerBlockUpdaterPtr PeerBlockUpdater;
    NDataNode::TPeerBlockDistributorPtr PeerBlockDistributor;
    NDataNode::TBlobReaderCachePtr BlobReaderCache;
    NDataNode::TJournalDispatcherPtr JournalDispatcher;
    NDataNode::TMasterConnectorPtr MasterConnector;
    NHiveClient::TClusterDirectoryPtr ClusterDirectory;
    NHiveClient::TClusterDirectorySynchronizerPtr ClusterDirectorySynchronizer;
    ICoreDumperPtr CoreDumper;

    NConcurrency::IThroughputThrottlerPtr TotalInThrottler;
    NConcurrency::IThroughputThrottlerPtr TotalOutThrottler;
    NConcurrency::IThroughputThrottlerPtr ReplicationInThrottler;
    NConcurrency::IThroughputThrottlerPtr ReplicationOutThrottler;
    NConcurrency::IThroughputThrottlerPtr RepairInThrottler;
    NConcurrency::IThroughputThrottlerPtr RepairOutThrottler;
    NConcurrency::IThroughputThrottlerPtr ArtifactCacheInThrottler;
    NConcurrency::IThroughputThrottlerPtr ArtifactCacheOutThrottler;
    NConcurrency::IThroughputThrottlerPtr SkynetOutThrottler;
    NConcurrency::IThroughputThrottlerPtr TabletCompactionAndPartitioningInThrottler;
    NConcurrency::IThroughputThrottlerPtr TabletCompactionAndPartitioningOutThrottler;
    NConcurrency::IThroughputThrottlerPtr TabletLoggingInThrottler;
    NConcurrency::IThroughputThrottlerPtr TabletPreloadOutThrottler;
    NConcurrency::IThroughputThrottlerPtr TabletSnapshotInThrottler;
    NConcurrency::IThroughputThrottlerPtr TabletStoreFlushInThrottler;
    NConcurrency::IThroughputThrottlerPtr TabletRecoveryOutThrottler;

    NTabletNode::TSlotManagerPtr TabletSlotManager;
    NTabletNode::TSecurityManagerPtr SecurityManager;
    NTabletNode::TInMemoryManagerPtr InMemoryManager;
    NTabletNode::TVersionedChunkMetaManagerPtr VersionedChunkMetaManager;

    NQueryClient::TColumnEvaluatorCachePtr ColumnEvaluatorCache;
    NQueryClient::ISubexecutorPtr QueryExecutor;

    void DoRun();
    void PopulateAlerts(std::vector<TError>* alerts);

    void OnMasterConnected();
    void OnMasterDisconnected();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
