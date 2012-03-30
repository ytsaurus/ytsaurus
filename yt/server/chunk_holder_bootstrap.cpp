#include "stdafx.h"
#include "chunk_holder_bootstrap.h"

#include <ytlib/misc/ref_counted_tracker.h>

#include <ytlib/bus/nl_server.h>

#include <ytlib/rpc/channel_cache.h>

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/virtual.h>

#include <ytlib/orchid/orchid_service.h>

#include <ytlib/monitoring/monitoring_manager.h>
#include <ytlib/monitoring/ytree_integration.h>
#include <ytlib/monitoring/http_server.h>
#include <ytlib/monitoring/http_integration.h>
#include <ytlib/monitoring/statlog.h>

#include <ytlib/ytree/yson_file_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/chunk_holder/chunk_holder_service.h>
#include <ytlib/chunk_holder/reader_cache.h>
#include <ytlib/chunk_holder/session_manager.h>
#include <ytlib/chunk_holder/block_store.h>
#include <ytlib/chunk_holder/peer_block_table.h>
#include <ytlib/chunk_holder/chunk_store.h>
#include <ytlib/chunk_holder/chunk_cache.h>
#include <ytlib/chunk_holder/chunk_registry.h>
#include <ytlib/chunk_holder/master_connector.h>
#include <ytlib/chunk_holder/peer_block_updater.h>
#include <ytlib/chunk_holder/ytree_integration.h>

namespace NYT {

static NLog::TLogger Logger("Server");

using NBus::IBusServer;
using NBus::TNLBusServerConfig;
using NBus::CreateNLBusServer;

using NRpc::IServer;
using NRpc::CreateRpcServer;
using NRpc::TChannelCache;

using NYTree::IYPathService;
using NYTree::SyncYPathSetNode;

using NMonitoring::TMonitoringManager;
using NMonitoring::GetYPathHttpHandler;
using NMonitoring::CreateMonitoringProvider;

using NOrchid::TOrchidService;

using NChunkHolder::TReaderCache;
using NChunkHolder::TChunkStore;
using NChunkHolder::TChunkCache;
using NChunkHolder::TChunkRegistry;
using NChunkHolder::TBlockStore;
using NChunkHolder::TPeerBlockTable;
using NChunkHolder::TSessionManager;
using NChunkHolder::TJobExecutor;
using NChunkHolder::TChunkHolderService;
using NChunkHolder::TMasterConnector;
using NChunkHolder::TPeerBlockUpdater;
using NChunkHolder::CreateStoredChunkMapService;
using NChunkHolder::CreateCachedChunkMapService;

////////////////////////////////////////////////////////////////////////////////

TChunkHolderBootstrap::TChunkHolderBootstrap(
    const Stroka& configFileName,
    TConfig* config)
    : ConfigFileName(configFileName)
    , Config(config)
{ }

void TChunkHolderBootstrap::Run()
{
    LOG_INFO("Starting chunk holder");

    // TODO: fixme
    // Explicitly instrumentation thread creation.
    //NSTAT::EnableStatlog(true);

    Config->PeerAddress = Sprintf("%s:%d", ~HostName(), Config->RpcPort);
    Config->CacheRemoteReader->PeerAddress = Config->PeerAddress;

    auto controlQueue = New<TActionQueue>("Control");

    auto busServer = CreateNLBusServer(~New<TNLBusServerConfig>(Config->RpcPort));

    auto rpcServer = CreateRpcServer(~busServer);

    auto readerCache = New<TReaderCache>(~Config);

    auto chunkRegistry = New<TChunkRegistry>();

    auto blockStore = New<TBlockStore>(
        ~Config,
        ~chunkRegistry,
        ~readerCache);

    auto blockTable = New<TPeerBlockTable>(~Config->PeerBlockTable);

    THolder<TChannelCache> channelCache(new TChannelCache());

    auto peerUpdater = New<TPeerBlockUpdater>(
        ~Config,
        ~blockStore,
        ~channelCache,
        ~controlQueue->GetInvoker());
    peerUpdater->Start();

    auto chunkStore = New<TChunkStore>(
        ~Config,
        ~readerCache);

    auto chunkCache = New<TChunkCache>(
        ~Config,
        ~readerCache,
        ~blockStore);

    chunkRegistry->SetChunkStore(~chunkStore);
    chunkRegistry->SetChunkCache(~chunkCache);

    auto sessionManager = New<TSessionManager>(
        ~Config,
        ~blockStore,
        ~chunkStore,
        ~controlQueue->GetInvoker());

    auto jobExecutor = New<TJobExecutor>(
        ~Config,
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
        ~busServer,
        ~chunkStore,
        ~chunkCache,
        ~readerCache,
        ~blockStore,
        ~blockTable,
        ~sessionManager);
    rpcServer->RegisterService(~chunkHolderService);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        BIND(&TRefCountedTracker::GetMonitoringInfo, TRefCountedTracker::Get()));
    monitoringManager->Register(
        "/bus_server",
        BIND(&IBusServer::GetMonitoringInfo, busServer));
    monitoringManager->Register(
        "/rpc_server",
        BIND(&IServer::GetMonitoringInfo, rpcServer));
    monitoringManager->Start();

    auto orchidFactory = NYTree::GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();
    SyncYPathSetNode(
        ~orchidRoot,
        "/monitoring",
        ~NYTree::CreateVirtualNode(~CreateMonitoringProvider(~monitoringManager)));
    SyncYPathSetNode(
        ~orchidRoot,
        "/config",
        ~NYTree::CreateVirtualNode(~NYTree::CreateYsonFileProvider(ConfigFileName)));
    SyncYPathSetNode(
        ~orchidRoot,
        "/stored_chunks",
        ~NYTree::CreateVirtualNode(~CreateStoredChunkMapService(~chunkStore)));
    SyncYPathSetNode(
        ~orchidRoot,
        "/cached_chunks",
        ~NYTree::CreateVirtualNode(~CreateCachedChunkMapService(~chunkCache)));

    auto orchidService = New<TOrchidService>(
        ~orchidRoot,
        ~controlQueue->GetInvoker());
    rpcServer->RegisterService(~orchidService);

    THolder<NHttp::TServer> httpServer(new NHttp::TServer(Config->MonitoringPort));
    httpServer->Register(
        "/statistics",
        ~NMonitoring::GetProfilingHttpHandler());
    httpServer->Register(
        "/orchid",
        ~NMonitoring::GetYPathHttpHandler(
            ~BIND([=] () -> IYPathService::TPtr
                {
                    return orchidRoot;
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
