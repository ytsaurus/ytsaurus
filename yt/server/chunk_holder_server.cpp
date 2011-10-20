#include "stdafx.h"
#include "chunk_holder_server.h"

namespace NYT {

static NLog::TLogger Logger("ChunkHolderServer");

////////////////////////////////////////////////////////////////////////////////

TChunkHolderServer::TChunkHolderServer(const TConfig &config)
    : Config(config)
{ }

void TChunkHolderServer::Run()
{
    LOG_INFO("Starting chunk holder on port %d",
        Config.Port);

    auto controlQueue = New<TActionQueue>();

    auto server = New<NRpc::TServer>(Config.Port);

    auto chunkHolder = New<TChunkHolder>(
        Config,
        controlQueue->GetInvoker(),
        server);

    server->Start();

    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
