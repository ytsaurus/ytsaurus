#include "stdafx.h"
#include "chunk_holder_server.h"

#include <yt/ytlib/bus/nl_server.h>

#include <yt/ytlib/ytree/tree_builder.h>
#include <yt/ytlib/ytree/ephemeral.h>
#include <yt/ytlib/ytree/virtual.h>

#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/ytlib/monitoring/monitoring_manager.h>
#include <yt/ytlib/monitoring/ytree_integration.h>
#include <yt/ytlib/monitoring/http_server.h>
#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/statlog.h>

#include <yt/ytlib/ytree/yson_file_service.h>
#include <yt/ytlib/ytree/ypath_client.h>

#include <yt/ytlib/chunk_holder/chunk_holder_service.h>
#include <yt/ytlib/chunk_holder/reader_cache.h>
#include <yt/ytlib/chunk_holder/session_manager.h>
#include <yt/ytlib/chunk_holder/block_store.h>
#include <yt/ytlib/chunk_holder/chunk_store.h>
#include <yt/ytlib/chunk_holder/chunk_cache.h>
#include <yt/ytlib/chunk_holder/master_connector.h>
#include <yt/ytlib/chunk_holder/ytree_integration.h>

namespace NYT {

static NLog::TLogger Logger("Server");

using NBus::IBusServer;
using NBus::TNLBusServerConfig;
using NBus::CreateNLBusServer;

using NRpc::IRpcServer;
using NRpc::CreateRpcServer;

using NYTree::IYPathService;
using NYTree::SyncYPathSetNode;

using NMonitoring::TMonitoringManager;
using NMonitoring::GetYPathHttpHandler;
using NMonitoring::CreateMonitoringProvider;

using NOrchid::TOrchidService;

using NChunkHolder::TReaderCache;
using NChunkHolder::TChunkStore;
using NChunkHolder::TChunkCache;
using NChunkHolder::TBlockStore;
using NChunkHolder::TSessionManager;
using NChunkHolder::TJobExecutor;
using NChunkHolder::TChunkHolderService;
using NChunkHolder::TMasterConnector;
using NChunkHolder::CreateChunkMapService;

////////////////////////////////////////////////////////////////////////////////

TChunkHolderServer::TChunkHolderServer(
    const Stroka& configFileName,
    TConfig* config)
    : ConfigFileName(configFileName)
    , Config(config)
{ }

void TChunkHolderServer::Run()
{
    LOG_INFO("Starting chunk holder");

    // TODO: fixme
    // Explicitly instrumentation thread creation.
    //NSTAT::EnableStatlog(true);

    auto controlQueue = New<TActionQueue>("Control");

    auto busServer = CreateNLBusServer(~New<TNLBusServerConfig>(Config->RpcPort));

    auto rpcServer = CreateRpcServer(~busServer);

    auto readerCache = New<TReaderCache>(~Config);

    auto chunkStore = New<TChunkStore>(
        ~Config,
        ~readerCache);

    auto chunkCache = New<TChunkCache>(
        ~Config,
        ~readerCache);

    auto blockStore = New<TBlockStore>(
        ~Config,
        ~chunkStore,
        ~chunkCache,
        ~readerCache);

    auto sessionManager = New<TSessionManager>(
        ~Config,
        ~blockStore,
        ~chunkStore,
        ~controlQueue->GetInvoker());

    auto jobExecutor = New<TJobExecutor>(
        ~chunkStore,
        ~blockStore,
        ~controlQueue->GetInvoker());

    auto masterConnector = New<TMasterConnector>(
        ~Config,
        ~chunkStore,
        ~chunkCache,
        ~sessionManager,
        ~jobExecutor,
        ~controlQueue->GetInvoker());

    auto chunkHolderService = New<TChunkHolderService>(
        ~Config,
        ~controlQueue->GetInvoker(),
        ~rpcServer,
        ~chunkStore,
        ~chunkCache,
        ~readerCache,
        ~blockStore,
        ~sessionManager);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        FromMethod(&TRefCountedTracker::GetMonitoringInfo, TRefCountedTracker::Get()));
    monitoringManager->Register(
        "/bus_server",
        FromMethod(&IBusServer::GetMonitoringInfo, busServer));
    monitoringManager->Register(
        "/rpc_server",
        FromMethod(&IRpcServer::GetMonitoringInfo, rpcServer));
    monitoringManager->Start();

    auto orchidFactory = NYTree::GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();
    auto orchidRootService = IYPathService::FromNode(~orchidRoot);
    SyncYPathSetNode(
        ~orchidRootService,
        "/monitoring",
        ~NYTree::CreateVirtualNode(~CreateMonitoringProvider(~monitoringManager)));
    SyncYPathSetNode(
        ~orchidRootService,
        "/config",
        ~NYTree::CreateVirtualNode(~NYTree::CreateYsonFileProvider(ConfigFileName)));
    SyncYPathSetNode(
        ~orchidRootService,
        "/chunks",
        ~NYTree::CreateVirtualNode(~CreateChunkMapService(~chunkStore)));

    auto orchidService = New<TOrchidService>(
        ~orchidRoot,
        ~rpcServer,
        ~controlQueue->GetInvoker());

    THolder<NHttp::TServer> httpServer(new NHttp::TServer(Config->MonitoringPort));
    auto orchidPathService = IYPathService::FromNode(~orchidRoot);
    httpServer->Register(
        "/statistics",
        ~NMonitoring::GetProfilingHttpHandler());
    httpServer->Register(
        "/orchid",
        ~NMonitoring::GetYPathHttpHandler(
            ~FromFunctor([=] () -> IYPathService::TPtr
                {
                    return orchidPathService;
                }),
            ~controlQueue->GetInvoker()));

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);
    httpServer->Start();

    LOG_INFO("Listening for RPC requests on port %d", Config->RpcPort);
    rpcServer->Start();

    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
