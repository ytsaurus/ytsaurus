#include "stdafx.h"
#include "chunk_holder_server.h"

#include <yt/ytlib/ytree/tree_builder.h>
#include <yt/ytlib/ytree/ephemeral.h>
#include <yt/ytlib/ytree/virtual.h>

#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/ytlib/monitoring/monitoring_manager.h>
#include <yt/ytlib/monitoring/cypress_integration.h>

#include <yt/ytlib/ytree/yson_file_service.h>

namespace NYT {

static NLog::TLogger Logger("ChunkHolder");

using NRpc::CreateRpcServer;

using NMonitoring::TMonitoringManager;

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

    auto rpcServer = CreateRpcServer(Config.Port);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/refcounted",
        FromMethod(&TRefCountedTracker::GetMonitoringInfo));

    // TODO: refactor
    auto orchidFactory = NYTree::GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();  
    orchidRoot->AddChild(
        NYTree::CreateVirtualNode(
            ~NMonitoring::CreateMonitoringProducer(~monitoringManager),
            orchidFactory),
        "monitoring");
    if (!Config.NewConfigFileName.empty()) {
        orchidRoot->AddChild(
            NYTree::CreateVirtualNode(
                ~NYTree::CreateYsonFileProducer(Config.NewConfigFileName),
                orchidFactory),
            "config");
    }

    auto orchidService = New<TOrchidService>(
        ~orchidRoot,
        ~rpcServer,
        ~controlQueue->GetInvoker());

    auto chunkHolder = New<TChunkHolder>(
        Config,
        ~controlQueue->GetInvoker(),
        ~rpcServer);

    rpcServer->Start();

    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
