#pragma once

#include "public.h"

#include <core/concurrency/throughput_throttler.h>
#include <core/concurrency/action_queue.h>

#include <core/bus/public.h>

#include <core/rpc/public.h>

#include <core/ytree/public.h>

#include <ytlib/monitoring/http_server.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/api/public.h>

#include <ytlib/query_client/public.h>

#include <server/data_node/public.h>

#include <server/chunk_server/public.h>

#include <server/job_agent/public.h>

#include <server/exec_agent/public.h>

#include <server/job_proxy/public.h>

#include <server/tablet_node/public.h>

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
    IInvokerPtr GetBoundedConcurrencyQueryPoolInvoker() const;
    IInvokerPtr GetBoundedConcurrencyReadPoolInvoker() const;
    NApi::IClientPtr GetMasterClient() const;
    NRpc::IServerPtr GetRpcServer() const;
    NRpc::IChannelFactoryPtr GetTabletChannelFactory() const;
    NYTree::IMapNodePtr GetOrchidRoot() const;
    NJobAgent::TJobControllerPtr GetJobController() const;
    NTabletNode::TSlotManagerPtr GetTabletSlotManager() const;
    NTabletNode::TSecurityManagerPtr GetSecurityManager() const;
    NTabletNode::TInMemoryManagerPtr GetInMemoryManager() const;
    NExecAgent::TSlotManagerPtr GetExecSlotManager() const;
    NExecAgent::TEnvironmentManagerPtr GetEnvironmentManager() const;
    NJobProxy::TJobProxyConfigPtr GetJobProxyConfig() const;
    TNodeMemoryTracker* GetMemoryUsageTracker() const;
    NDataNode::TChunkStorePtr GetChunkStore() const;
    NDataNode::TChunkCachePtr GetChunkCache() const;
    NDataNode::TChunkRegistryPtr GetChunkRegistry() const;
    NDataNode::TSessionManagerPtr GetSessionManager() const;
    NDataNode::TBlockStorePtr GetBlockStore() const;
    NChunkClient::IBlockCachePtr GetBlockCache() const;
    NDataNode::TPeerBlockTablePtr GetPeerBlockTable() const;
    NDataNode::TBlobReaderCachePtr GetBlobReaderCache() const;
    NDataNode::TJournalDispatcherPtr GetJournalDispatcher() const;
    NDataNode::TMasterConnectorPtr GetMasterConnector() const;
    NQueryClient::IExecutorPtr GetQueryExecutor() const;

    NConcurrency::IThroughputThrottlerPtr GetReplicationInThrottler() const;
    NConcurrency::IThroughputThrottlerPtr GetReplicationOutThrottler() const;
    NConcurrency::IThroughputThrottlerPtr GetRepairInThrottler() const;
    NConcurrency::IThroughputThrottlerPtr GetRepairOutThrottler() const;

    NConcurrency::IThroughputThrottlerPtr GetInThrottler(NChunkClient::EWriteSessionType sessionType) const;
    NConcurrency::IThroughputThrottlerPtr GetOutThrottler(NChunkClient::EWriteSessionType sessionType) const;
    NConcurrency::IThroughputThrottlerPtr GetOutThrottler(NChunkClient::EReadSessionType sessionType) const;

    const NElection::TCellId& GetCellId() const;

    void Run();

private:
    const NYTree::INodePtr ConfigNode;

    TCellNodeConfigPtr Config;

    NConcurrency::TActionQueuePtr ControlQueue;

    NConcurrency::TThreadPoolPtr QueryThreadPool;
    IInvokerPtr BoundedConcurrencyQueryPoolInvoker;
    IInvokerPtr BoundedConcurrencyReadPoolInvoker;

    NBus::IBusServerPtr BusServer;
    NApi::IClientPtr MasterClient;
    NRpc::IServerPtr RpcServer;
    std::unique_ptr<NHttp::TServer> HttpServer;
    NRpc::IChannelFactoryPtr TabletChannelFactory;
    NYTree::IMapNodePtr OrchidRoot;
    NJobAgent::TJobControllerPtr JobController;
    NExecAgent::TSlotManagerPtr ExecSlotManager;
    NExecAgent::TEnvironmentManagerPtr EnvironmentManager;
    NJobProxy::TJobProxyConfigPtr JobProxyConfig;
    std::unique_ptr<TMemoryUsageTracker<NNodeTrackerClient::EMemoryCategory>> MemoryUsageTracker;
    NExecAgent::TSchedulerConnectorPtr SchedulerConnector;
    NDataNode::TChunkStorePtr ChunkStore;
    NDataNode::TChunkCachePtr ChunkCache;
    NDataNode::TChunkRegistryPtr ChunkRegistry;
    NDataNode::TSessionManagerPtr SessionManager;
    NDataNode::TBlockStorePtr BlockStore;
    NChunkClient::IBlockCachePtr BlockCache;
    NDataNode::TPeerBlockTablePtr PeerBlockTable;
    NDataNode::TPeerBlockUpdaterPtr PeerBlockUpdater;
    NDataNode::TBlobReaderCachePtr BlobReaderCache;
    NDataNode::TJournalDispatcherPtr JournalDispatcher;
    NDataNode::TMasterConnectorPtr MasterConnector;

    NConcurrency::IThroughputThrottlerPtr ReplicationInThrottler;
    NConcurrency::IThroughputThrottlerPtr ReplicationOutThrottler;
    NConcurrency::IThroughputThrottlerPtr RepairInThrottler;
    NConcurrency::IThroughputThrottlerPtr RepairOutThrottler;

    NTabletNode::TSlotManagerPtr TabletSlotManager;
    NTabletNode::TSecurityManagerPtr SecurityManager;
    NTabletNode::TInMemoryManagerPtr InMemoryManager;

    NQueryClient::IExecutorPtr QueryExecutor;


    void DoRun();
    NNodeTrackerClient::TAddressMap GetLocalAddresses();
    void PopulateAlerts(std::vector<TError>* alerts);
    NElection::TCellId ToRedirectorCellId(const NElection::TCellId& cellId);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
