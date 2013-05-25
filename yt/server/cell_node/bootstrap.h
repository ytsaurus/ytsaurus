#pragma once

#include "public.h"

#include <ytlib/misc/throughput_throttler.h>

#include <ytlib/actions/action_queue.h>

#include <ytlib/bus/public.h>

#include <ytlib/rpc/public.h>

#include <ytlib/ytree/public.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <server/chunk_holder/public.h>

#include <server/chunk_server/public.h>

#include <server/job_agent/public.h>

#include <server/exec_agent/public.h>

#include <server/job_proxy/public.h>

namespace NYT {
namespace NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(
        const Stroka& configFileName,
        TCellNodeConfigPtr config);
    ~TBootstrap();

    TCellNodeConfigPtr GetConfig() const;
    IInvokerPtr GetControlInvoker() const;
    NRpc::IChannelPtr GetMasterChannel() const;
    NRpc::IChannelPtr GetSchedulerChannel() const;
    NRpc::IServerPtr GetRpcServer() const;
    NYTree::IMapNodePtr GetOrchidRoot() const;
    NJobAgent::TJobTrackerPtr GetJobController() const;
    NExecAgent::TSlotManagerPtr GetSlotManager() const;
    NExecAgent::TEnvironmentManagerPtr GetEnvironmentManager() const;
    NJobProxy::TJobProxyConfigPtr GetJobProxyConfig() const;
    TNodeMemoryTracker& GetMemoryUsageTracker();
    NChunkHolder::TChunkStorePtr GetChunkStore() const;
    NChunkHolder::TChunkCachePtr GetChunkCache() const;
    NChunkHolder::TChunkRegistryPtr GetChunkRegistry() const;
    NChunkHolder::TSessionManagerPtr GetSessionManager() const;
    NChunkHolder::TBlockStorePtr GetBlockStore() const;
    NChunkHolder::TPeerBlockTablePtr GetPeerBlockTable() const;
    NChunkHolder::TReaderCachePtr GetReaderCache() const;
    NChunkHolder::TMasterConnectorPtr GetMasterConnector() const;

    IThroughputThrottlerPtr GetReplicationInThrottler() const;
    IThroughputThrottlerPtr GetReplicationOutThrottler() const;
    IThroughputThrottlerPtr GetRepairInThrottler() const;
    IThroughputThrottlerPtr GetRepairOutThrottler() const;
    
    IThroughputThrottlerPtr GetInThrottler(NChunkClient::EWriteSessionType sessionType) const;
    IThroughputThrottlerPtr GetOutThrottler(NChunkClient::EWriteSessionType sessionType) const;
    IThroughputThrottlerPtr GetOutThrottler(NChunkClient::EReadSessionType sessionType) const;

    const NNodeTrackerClient::TNodeDescriptor& GetLocalDescriptor() const;

    const TGuid& GetCellGuid() const;
    void UpdateCellGuid(const TGuid& cellGuid);

    void Run();

private:
    Stroka ConfigFileName;
    TCellNodeConfigPtr Config;

    TActionQueuePtr ControlQueue;
    NBus::IBusServerPtr BusServer;
    NRpc::IChannelPtr MasterChannel;
    NRpc::IChannelPtr SchedulerChannel;
    NRpc::IServerPtr RpcServer;
    NYTree::IMapNodePtr OrchidRoot;
    NJobAgent::TJobTrackerPtr JobController;
    NExecAgent::TSlotManagerPtr SlotManager;
    NExecAgent::TEnvironmentManagerPtr EnvironmentManager;
    NJobProxy::TJobProxyConfigPtr JobProxyConfig;
    TMemoryUsageTracker<EMemoryConsumer> MemoryUsageTracker;
    NExecAgent::TSchedulerConnectorPtr SchedulerConnector;
    NChunkHolder::TChunkStorePtr ChunkStore;
    NChunkHolder::TChunkCachePtr ChunkCache;
    NChunkHolder::TChunkRegistryPtr ChunkRegistry;
    NChunkHolder::TSessionManagerPtr SessionManager;
    NChunkHolder::TBlockStorePtr BlockStore;
    NChunkHolder::TPeerBlockTablePtr PeerBlockTable;
    NChunkHolder::TPeerBlockUpdaterPtr PeerBlockUpdater;
    NChunkHolder::TReaderCachePtr ReaderCache;
    NChunkHolder::TMasterConnectorPtr MasterConnector;
    IThroughputThrottlerPtr ReplicationInThrottler;
    IThroughputThrottlerPtr ReplicationOutThrottler;
    IThroughputThrottlerPtr RepairInThrottler;
    IThroughputThrottlerPtr RepairOutThrottler;

    NNodeTrackerClient::TNodeDescriptor LocalDescriptor;
    TGuid CellGuid;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
