#pragma once

#include "public.h"

#include <ytlib/actions/invoker.h>
#include <ytlib/cell_node/public.h>
// TODO(babenko): replace with public.h
#include <ytlib/bus/server.h>
// TODO(babenko): replace with public.h
#include <ytlib/rpc/channel.h>

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
    IInvoker::TPtr GetControlInvoker() const;
    TBlockStorePtr GetBlockStore();
    NBus::IBusServer::TPtr GetBusServer() const;
    TPeerBlockTablePtr GetPeerBlockTable() const;
    TReaderCachePtr GetReaderCache() const;
    NRpc::IChannel::TPtr GetMasterChannel() const;
    Stroka GetPeerAddress() const;

private:
	TChunkHolderConfigPtr Config;
	NCellNode::TBootstrap* NodeBootstrap;
    
    NChunkHolder::TChunkStorePtr ChunkStore;
    NChunkHolder::TChunkCachePtr ChunkCache;
    NChunkHolder::TSessionManagerPtr SessionManager;
    NChunkHolder::TJobExecutorPtr JobExecutor;
    NChunkHolder::TBlockStorePtr BlockStore;
    NChunkHolder::TPeerBlockTablePtr PeerBlockTable;
    NChunkHolder::TReaderCachePtr ReaderCache;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
