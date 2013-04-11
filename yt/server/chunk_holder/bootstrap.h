#pragma once

#include "public.h"

#include <ytlib/actions/action_queue.h>

#include <ytlib/bus/public.h>

#include <ytlib/rpc/public.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <server/misc/memory_usage_tracker.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* nodeBootstrap);
    ~TBootstrap();

    void Init();

    TDataNodeConfigPtr GetConfig() const;
    TChunkStorePtr GetChunkStore() const;
    TChunkCachePtr GetChunkCache() const;
    TSessionManagerPtr GetSessionManager() const;
    TJobExecutorPtr GetJobExecutor() const;
    IInvokerPtr GetControlInvoker() const;
    TChunkRegistryPtr GetChunkRegistry() const;
    TBlockStorePtr GetBlockStore();
    TPeerBlockTablePtr GetPeerBlockTable() const;
    TReaderCachePtr GetReaderCache() const;
    TMasterConnectorPtr GetMasterConnector() const;
    NRpc::IChannelPtr GetMasterChannel() const;
    const NNodeTrackerClient::TNodeDescriptor& GetLocalDescriptor() const;
    TMemoryUsageTracker<NCellNode::EMemoryConsumer>& GetMemoryUsageTracker();
    const TGuid& GetCellGuid() const;
    void UpdateCellGuid(const TGuid& cellGuid);

private:
    TDataNodeConfigPtr Config;
    NCellNode::TBootstrap* NodeBootstrap;

    TChunkRegistryPtr ChunkRegistry;
    TChunkStorePtr ChunkStore;
    TChunkCachePtr ChunkCache;
    TSessionManagerPtr SessionManager;
    TJobExecutorPtr JobExecutor;
    TBlockStorePtr BlockStore;
    TPeerBlockTablePtr PeerBlockTable;
    TPeerBlockUpdaterPtr PeerBlockUpdater;
    TReaderCachePtr ReaderCache;
    TMasterConnectorPtr MasterConnector;

    TGuid CellGuid;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
