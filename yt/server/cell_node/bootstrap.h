#pragma once

#include "public.h"

#include <yt/server/chunk_server/public.h>

#include <yt/server/exec_agent/public.h>

#include <yt/server/data_node/public.h>

#include <yt/server/job_agent/public.h>

#include <yt/server/job_proxy/public.h>

#include <yt/server/tablet_node/public.h>

#include <yt/server/hive/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/misc/public.h>

#include <yt/ytlib/monitoring/http_server.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/query_client/public.h>

#include <yt/core/bus/public.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/rpc/public.h>

#include <yt/core/ytree/public.h>

#include <yt/core/misc/public.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    explicit TBootstrap(NYTree::INodePtr configNode);
    ~TBootstrap();

    TCellNodeConfigPtr GetConfig() const;
    IInvokerPtr GetControlInvoker() const;
    IInvokerPtr GetQueryPoolInvoker() const;
    NApi::INativeClientPtr GetMasterClient() const;
    NRpc::IServerPtr GetRpcServer() const;
    NRpc::IChannelFactoryPtr GetTabletChannelFactory() const;
    NYTree::IMapNodePtr GetOrchidRoot() const;
    NJobAgent::TJobControllerPtr GetJobController() const;
    NTabletNode::TSlotManagerPtr GetTabletSlotManager() const;
    NTabletNode::TSecurityManagerPtr GetSecurityManager() const;
    NTabletNode::TInMemoryManagerPtr GetInMemoryManager() const;
    NExecAgent::TSlotManagerPtr GetExecSlotManager() const;
    NJobProxy::TJobProxyConfigPtr GetJobProxyConfig() const;
    TNodeMemoryTracker* GetMemoryUsageTracker() const;
    NDataNode::TChunkStorePtr GetChunkStore() const;
    NDataNode::TChunkCachePtr GetChunkCache() const;
    NDataNode::TChunkRegistryPtr GetChunkRegistry() const;
    NDataNode::TSessionManagerPtr GetSessionManager() const;
    NDataNode::TChunkMetaManagerPtr GetChunkMetaManager() const;
    NDataNode::TChunkBlockManagerPtr GetChunkBlockManager() const;
    NChunkClient::IBlockCachePtr GetBlockCache() const;
    NDataNode::TPeerBlockTablePtr GetPeerBlockTable() const;
    NDataNode::TBlobReaderCachePtr GetBlobReaderCache() const;
    NDataNode::TJournalDispatcherPtr GetJournalDispatcher() const;
    NDataNode::TMasterConnectorPtr GetMasterConnector() const;
    NQueryClient::TColumnEvaluatorCachePtr GetColumnEvaluatorCache() const;
    NQueryClient::ISubexecutorPtr GetQueryExecutor() const;

    NConcurrency::IThroughputThrottlerPtr GetReplicationInThrottler() const;
    NConcurrency::IThroughputThrottlerPtr GetReplicationOutThrottler() const;
    NConcurrency::IThroughputThrottlerPtr GetRepairInThrottler() const;
    NConcurrency::IThroughputThrottlerPtr GetRepairOutThrottler() const;
    NConcurrency::IThroughputThrottlerPtr GetArtifactCacheInThrottler() const;
    NConcurrency::IThroughputThrottlerPtr GetArtifactCacheOutThrottler() const;

    NConcurrency::IThroughputThrottlerPtr GetInThrottler(const TWorkloadDescriptor& descriptor) const;
    NConcurrency::IThroughputThrottlerPtr GetOutThrottler(const TWorkloadDescriptor& descriptor) const;

    const NObjectClient::TCellId& GetCellId() const;
    NObjectClient::TCellId GetCellId(NObjectClient::TCellTag cellTag) const;
    NNodeTrackerClient::TAddressMap GetLocalAddresses();
    NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks();

    void Run();

private:
    const NYTree::INodePtr ConfigNode;

    TCellNodeConfigPtr Config;

    NConcurrency::TActionQueuePtr ControlQueue;

    NConcurrency::TThreadPoolPtr QueryThreadPool;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    std::unique_ptr<NLFAlloc::TLFAllocProfiler> LFAllocProfiler_;
    NBus::IBusServerPtr BusServer;
    NApi::INativeConnectionPtr MasterConnection;
    NApi::INativeClientPtr MasterClient;
    NHiveServer::TCellDirectorySynchronizerPtr CellDirectorySynchronizer;
    NRpc::IServerPtr RpcServer;
    std::unique_ptr<NHttp::TServer> HttpServer;
    NRpc::IChannelFactoryPtr TabletChannelFactory;
    NYTree::IMapNodePtr OrchidRoot;
    NJobAgent::TJobControllerPtr JobController;
    NExecAgent::TSlotManagerPtr ExecSlotManager;
    NJobProxy::TJobProxyConfigPtr JobProxyConfig;
    std::unique_ptr<TMemoryUsageTracker<NNodeTrackerClient::EMemoryCategory>> MemoryUsageTracker;
    NExecAgent::TSchedulerConnectorPtr SchedulerConnector;
    NDataNode::TChunkStorePtr ChunkStore;
    NDataNode::TChunkCachePtr ChunkCache;
    NDataNode::TChunkRegistryPtr ChunkRegistry;
    NDataNode::TSessionManagerPtr SessionManager;
    NDataNode::TChunkMetaManagerPtr ChunkMetaManager;
    NDataNode::TChunkBlockManagerPtr ChunkBlockManager;
    NChunkClient::IBlockCachePtr BlockCache;
    NDataNode::TPeerBlockTablePtr PeerBlockTable;
    NDataNode::TPeerBlockUpdaterPtr PeerBlockUpdater;
    NDataNode::TBlobReaderCachePtr BlobReaderCache;
    NDataNode::TJournalDispatcherPtr JournalDispatcher;
    NDataNode::TMasterConnectorPtr MasterConnector;

    NConcurrency::IThroughputThrottlerPtr TotalInThrottler;
    NConcurrency::IThroughputThrottlerPtr TotalOutThrottler;
    NConcurrency::IThroughputThrottlerPtr ReplicationInThrottler;
    NConcurrency::IThroughputThrottlerPtr ReplicationOutThrottler;
    NConcurrency::IThroughputThrottlerPtr RepairInThrottler;
    NConcurrency::IThroughputThrottlerPtr RepairOutThrottler;
    NConcurrency::IThroughputThrottlerPtr ArtifactCacheInThrottler;
    NConcurrency::IThroughputThrottlerPtr ArtifactCacheOutThrottler;

    NTabletNode::TSlotManagerPtr TabletSlotManager;
    NTabletNode::TSecurityManagerPtr SecurityManager;
    NTabletNode::TInMemoryManagerPtr InMemoryManager;

    NQueryClient::TColumnEvaluatorCachePtr ColumnEvaluatorCache;
    NQueryClient::ISubexecutorPtr QueryExecutor;


    void DoRun();
    void PopulateAlerts(std::vector<TError>* alerts);
    NObjectClient::TCellId ToRedirectorCellId(const NObjectClient::TCellId& cellId);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
