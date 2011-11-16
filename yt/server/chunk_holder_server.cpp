#include "stdafx.h"
#include "chunk_holder_server.h"

#include <yt/ytlib/ytree/tree_builder.h>
#include <yt/ytlib/ytree/ephemeral.h>
#include <yt/ytlib/ytree/fluent.h>

#include <yt/ytlib/orchid/orchid_service.h>

namespace NYT {

static NLog::TLogger Logger("Holder");

using NRpc::CreateRpcServer;

using NOrchid::TOrchidService;

////////////////////////////////////////////////////////////////////////////////

TChunkHolderServer::TChunkHolderServer(const TConfig &config)
    : Config(config)
{ }

void TChunkHolderServer::Run()
{
    LOG_INFO("Starting chunk holder on port %d",
        Config.Port);

    auto controlQueue = New<TActionQueue>();

    auto server = CreateRpcServer(Config.Port);

    auto orchidBuilder = NYTree::CreateBuilderFromFactory(NYTree::GetEphemeralNodeFactory());
    orchidBuilder->BeginTree();
    NYTree::BuildYsonFluently(~orchidBuilder)
        // TODO: test
        .BeginMap()
            .Item("hello").Scalar("world")
        .EndMap();
    auto orchidRoot = orchidBuilder->EndTree();

    auto orchidService = New<TOrchidService>(
        ~orchidRoot,
        ~server,
        ~controlQueue->GetInvoker());

    auto chunkHolder = New<TChunkHolder>(
        Config,
        ~controlQueue->GetInvoker(),
        ~server);

    server->Start();

    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
