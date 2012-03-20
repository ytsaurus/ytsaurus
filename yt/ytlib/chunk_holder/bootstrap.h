#pragma once

#include "public.h"

#include <ytlib/actions/invoker.h>
//#include <ytlib/misc/guid.h>
#include <ytlib/cell_node/public.h>
// TODO(babenko): replace with public.h
//#include <ytlib/bus/server.h>
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

    TChunkHolderConfigPtr GetConfig() const;
    // TODO(babenko): move to node bootstrap
    TIncarnationId GetIncarnationId() const;
    TChunkStorePtr GetChunkStore() const;
    TChunkCachePtr GetChunkCache() const;
    TSessionManagerPtr GetSessionManager() const;
    TJobExecutorPtr GetJobExecutor() const;
    IInvoker::TPtr GetControlInvoker() const;
    NChunkHolder::TBlockStorePtr GetBlockStore();
    //NBus::IBusServer::TPtr GetBusServer() const;
    NChunkHolder::TPeerBlockTablePtr GetPeerBlockTable() const;
    NChunkHolder::TReaderCachePtr GetReaderCache() const;
    NRpc::IChannel::TPtr GetLeaderChannel() const;
    Stroka GetPeerAddress() const;

private:
	TChunkHolderConfigPtr Config;
	NCellNode::TBootstrap* NodeBootstrap;
    
    NChunkServer::TIncarnationId IncarnationId;
    NChunkHolder::TChunkStorePtr ChunkStore;
    NChunkHolder::TChunkCachePtr ChunkCache;
    NChunkHolder::TSessionManagerPtr SessionManager;
    NChunkHolder::TJobExecutorPtr JobExecutor;
    IInvoker::TPtr ControlInvoker;
    NChunkHolder::TBlockStorePtr BlockStore;
    NBus::IBusServer::TPtr BusServer;
    NChunkHolder::TPeerBlockTablePtr PeerBlockTable;
    NChunkHolder::TReaderCachePtr ReaderCache;
    NRpc::IChannel::TPtr LeaderChannel;
    Stroka PeerAddress;
    NExecAgent::TJobManagerPtr ExecJobManager;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
