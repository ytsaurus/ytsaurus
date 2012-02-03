#pragma once

#include "config.h"

#include <ytlib/actions/invoker.h>
#include <ytlib/misc/guid.h>
#include <ytlib/chunk_server/id.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TChunkStore;
class TChunkCache;
class TSessionManager;
class TJobExecutor;

class TBootstrap
{
public:
    typedef NChunkHolder::TChunkHolderConfig TConfig;

    TBootstrap(
        const Stroka& configFileName,
        TConfig* config);
    ~TBootstrap();

    TConfig* GetConfig() const;
    NChunkServer::TIncarnationId GetIncarnationId() const;
    TChunkStore* GetChunkStore() const;
    TChunkCache* GetChunkCache() const;
    TSessionManager* GetSessionManager() const;
    TJobExecutor* GetJobExecutor() const;
    IInvoker* GetServiceInvoker() const;

    void Run();

private:
    Stroka ConfigFileName;
    TConfig::TPtr Config;
    
    NChunkServer::TIncarnationId IncarnationId;
    TIntrusivePtr<TChunkStore> ChunkStore;
    TIntrusivePtr<TChunkCache> ChunkCache;
    TIntrusivePtr<TSessionManager> SessionManager;
    TIntrusivePtr<TJobExecutor> JobExecutor;
    TIntrusivePtr<IInvoker> ServiceInvoker;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
