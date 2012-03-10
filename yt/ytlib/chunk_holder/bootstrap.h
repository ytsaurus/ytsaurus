#pragma once

#include "config.h"
#include "public.h"

#include <ytlib/actions/invoker.h>
#include <ytlib/misc/guid.h>
#include <ytlib/chunk_server/id.h>
// TODO(babenko): replace with public.h
#include <ytlib/bus/server.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    typedef NChunkHolder::TChunkHolderConfig TConfig;
    typedef NChunkHolder::TChunkHolderConfigPtr TConfigPtr;

    TBootstrap(
        const Stroka& configFileName,
        TConfig* config);
    ~TBootstrap();

    TConfigPtr GetConfig() const;
    NChunkServer::TIncarnationId GetIncarnationId() const;
    TChunkStorePtr GetChunkStore() const;
    TChunkCachePtr GetChunkCache() const;
    TSessionManagerPtr GetSessionManager() const;
    TJobExecutorPtr GetJobExecutor() const;
    IInvoker::TPtr GetControlInvoker() const;
    TBlockStorePtr GetBlockStore();
    NBus::IBusServer::TPtr GetBusServer() const;
    TPeerBlockTablePtr GetPeerBlockTable() const;

    void Run();

private:
    Stroka ConfigFileName;
    TConfigPtr Config;
    
    NChunkServer::TIncarnationId IncarnationId;
    TChunkStorePtr ChunkStore;
    TChunkCachePtr ChunkCache;
    TSessionManagerPtr SessionManager;
    TJobExecutorPtr JobExecutor;
    IInvoker::TPtr ControlInvoker;
    TBlockStorePtr BlockStore;
    NBus::IBusServer::TPtr BusServer;
    TPeerBlockTablePtr PeerBlockTable;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
