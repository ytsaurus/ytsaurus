#pragma once

#include "public.h"

#include <ytlib/actions/action_queue.h>
#include <ytlib/cell_node/public.h>
#include <ytlib/bus/public.h>
#include <ytlib/rpc/public.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(
        TChunkHolderConfigPtr config,
        NCellNode::TBootstrap* nodeBootstrap);
    ~TBootstrap();

    void Init();

    TChunkHolderConfigPtr GetConfig() const;
    NChunkServer::TIncarnationId GetIncarnationId() const;
    TChunkStorePtr GetChunkStore() const;
    TChunkCachePtr GetChunkCache() const;
    TSessionManagerPtr GetSessionManager() const;
    TJobExecutorPtr GetJobExecutor() const;
    IInvokerPtr GetControlInvoker() const;
    IInvokerPtr GetReadPoolInvoker() const;
    IInvokerPtr GetWritePoolInvoker() const;
    TChunkRegistryPtr GetChunkRegistry() const;
    TBlockStorePtr GetBlockStore();
    TPeerBlockTablePtr GetPeerBlockTable() const;
    TReaderCachePtr GetReaderCache() const;
    NRpc::IChannelPtr GetMasterChannel() const;
    Stroka GetPeerAddress() const;

private:
    TChunkHolderConfigPtr Config;
    NCellNode::TBootstrap* NodeBootstrap;
    
    TThreadPoolPtr ReadThreadPool;
    TThreadPoolPtr WriteThreadPool;
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
    
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
