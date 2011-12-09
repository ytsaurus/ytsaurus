#include "stdafx.h"
#include "chunk_holder_server.h"

#include <yt/ytlib/bus/nl_server.h>

#include <yt/ytlib/ytree/tree_builder.h>
#include <yt/ytlib/ytree/ephemeral.h>
#include <yt/ytlib/ytree/virtual.h>

#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/ytlib/monitoring/monitoring_manager.h>
#include <yt/ytlib/monitoring/ytree_integration.h>
#include <yt/ytlib/monitoring/http_tree_server.h>

#include <yt/ytlib/ytree/yson_file_service.h>

#include <yt/ytlib/chunk_holder/chunk_holder_service.h>
#include <yt/ytlib/chunk_holder/session_manager.h>
#include <yt/ytlib/chunk_holder/block_store.h>
#include <yt/ytlib/chunk_holder/chunk_store.h>
#include <yt/ytlib/chunk_holder/master_connector.h>
#include <yt/ytlib/chunk_holder/ytree_integration.h>

namespace NYT {

static NLog::TLogger Logger("ChunkHolder");

using NBus::IBusServer;
using NBus::TNLBusServerConfig;
using NBus::CreateNLBusServer;

using NRpc::IRpcServer;
using NRpc::CreateRpcServer;

using NYTree::IYPathService;

using NMonitoring::TMonitoringManager;
using NMonitoring::THttpTreeServer;
using NMonitoring::GetYPathHttpHandler;
using NMonitoring::CreateMonitoringProvider;

using NOrchid::TOrchidService;

using NChunkHolder::TChunkStore;
using NChunkHolder::TBlockStore;
using NChunkHolder::TSessionManager;
using NChunkHolder::TReplicator;
using NChunkHolder::TChunkHolderService;
using NChunkHolder::TMasterConnector;
using NChunkHolder::CreateChunkMapService;

////////////////////////////////////////////////////////////////////////////////

TChunkHolderServer::TChunkHolderServer(const TConfig &config)
    : Config(config)
{ }

void TChunkHolderServer::Run()
{
    LOG_INFO("Starting chunk holder");

    auto controlQueue = New<TActionQueue>();

    auto busServer = CreateNLBusServer(TNLBusServerConfig(Config.RpcPort));

    auto rpcServer = CreateRpcServer(~busServer);

    auto chunkStore = New<TChunkStore>(Config);
    auto blockStore = New<TBlockStore>(Config, chunkStore);

    auto sessionManager = New<TSessionManager>(
        Config,
        blockStore,
        chunkStore,
        ~controlQueue->GetInvoker());

    auto replicator = New<TReplicator>(
        chunkStore,
        blockStore,
        ~controlQueue->GetInvoker());

    if (!Config.Masters.Addresses.empty()) {
        auto masterConnector = New<TMasterConnector>(
            Config,
            ~chunkStore,
            ~sessionManager,
            ~replicator,
            ~controlQueue->GetInvoker());
    } else {
        LOG_INFO("Running in standalone mode");
    }

    auto chunkHolderService = New<TChunkHolderService>(
        Config,
        ~controlQueue->GetInvoker(),
        ~rpcServer,
        ~chunkStore,
        ~blockStore,
        ~sessionManager);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        FromMethod(&TRefCountedTracker::GetMonitoringInfo));
    monitoringManager->Register(
        "/bus_server",
        FromMethod(&IBusServer::GetMonitoringInfo, busServer));
    monitoringManager->Register(
        "/rpc_server",
        FromMethod(&IRpcServer::GetMonitoringInfo, rpcServer));
    monitoringManager->Start();

    // TODO: refactor
    auto orchidFactory = NYTree::GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();  
    YVERIFY(orchidRoot->AddChild(
        NYTree::CreateVirtualNode(
            ~CreateMonitoringProvider(~monitoringManager),
            orchidFactory),
        "monitoring"));
    YVERIFY(orchidRoot->AddChild(
        NYTree::CreateVirtualNode(
            ~CreateChunkMapService(~chunkStore),
            orchidFactory),
        "chunks"));
    if (!Config.NewConfigFileName.empty()) {
        YVERIFY(orchidRoot->AddChild(
            NYTree::CreateVirtualNode(
                ~NYTree::CreateYsonFileProvider(Config.NewConfigFileName),
                orchidFactory),
            "config"));
    }

    auto orchidService = New<TOrchidService>(
        ~orchidRoot,
        ~rpcServer,
        ~controlQueue->GetInvoker());

    // TODO: fix memory leaking
    auto httpServer = new THttpTreeServer(Config.MonitoringPort);
    auto orchidPathService = ToFuture(IYPathService::FromNode(~orchidRoot));
    httpServer->Register(
        "orchid",
        GetYPathHttpHandler(
            ~FromFunctor([=] () -> TFuture<IYPathService::TPtr>::TPtr
                {
                    return orchidPathService;
                })));

    LOG_INFO("Listening for HTTP monitoring requests on port %d", Config.MonitoringPort);
    httpServer->Start();

    LOG_INFO("Listening for RPC requests on port %d", Config.RpcPort);
    rpcServer->Start();

    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
