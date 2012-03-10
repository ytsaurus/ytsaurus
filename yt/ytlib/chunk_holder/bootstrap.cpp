#include "stdafx.h"
#include "bootstrap.h"
#include "chunk_holder_service.h"
#include "reader_cache.h"
#include "session_manager.h"
#include "block_store.h"
#include "peer_block_table.h"
#include "chunk_store.h"
#include "chunk_cache.h"
#include "chunk_registry.h"
#include "master_connector.h"
#include "job_executor.h"
#include "peer_block_updater.h"
#include "ytree_integration.h"

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
#include <ytlib/ytree/yson_file_service.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/profiling/profiling_manager.h>

namespace NYT {
namespace NChunkHolder {

using namespace NBus;
using namespace NRpc;
using namespace NYTree;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NChunkServer;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChunkHolder");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    const Stroka& configFileName,
    TConfig* config)
    : ConfigFileName(configFileName)
    , Config(config)
{ }

TBootstrap::~TBootstrap()
{ }

void TBootstrap::Run()
{
    IncarnationId = TIncarnationId::Create();

    Config->PeerAddress = Sprintf("%s:%d", ~HostName(), Config->RpcPort);
    Config->CacheRemoteReader->PeerAddress = Config->PeerAddress;

    LOG_INFO("Starting chunk holder (IncarnationId: %s)", ~IncarnationId.ToString());

    auto controlQueue = New<TActionQueue>("Control");
    ControlInvoker = controlQueue->GetInvoker();

    BusServer = CreateNLBusServer(~New<TNLBusServerConfig>(Config->RpcPort));

    auto rpcServer = CreateRpcServer(~BusServer);

    auto readerCache = New<TReaderCache>(~Config);

    auto chunkRegistry = New<TChunkRegistry>();

    BlockStore = New<TBlockStore>(
        ~Config,
        ~chunkRegistry,
        ~readerCache);

    PeerBlockTable = New<TPeerBlockTable>(~Config->PeerBlockTable);

    auto peerUpdater = New<TPeerBlockUpdater>(
        ~Config,
        ~BlockStore,
        controlQueue->GetInvoker());
    peerUpdater->Start();

    ChunkStore = New<TChunkStore>(
        ~Config,
        ~readerCache);

    ChunkCache = New<TChunkCache>(
        ~Config,
        ~readerCache,
        ~BlockStore);

    chunkRegistry->SetChunkStore(~ChunkStore);
    chunkRegistry->SetChunkCache(~ChunkCache);

    SessionManager = New<TSessionManager>(
        ~Config,
        ~BlockStore,
        ~ChunkStore,
        controlQueue->GetInvoker());

    JobExecutor = New<TJobExecutor>(
        ~Config,
        ~ChunkStore,
        ~BlockStore,
        controlQueue->GetInvoker());

    auto masterConnector = New<TMasterConnector>(~Config, this);

    auto chunkHolderService = New<TChunkHolderService>(~Config, this);
    rpcServer->RegisterService(~chunkHolderService);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "ref_counted",
        FromMethod(&TRefCountedTracker::GetMonitoringInfo, TRefCountedTracker::Get()));
    monitoringManager->Register(
        "bus_server",
        FromMethod(&IBusServer::GetMonitoringInfo, BusServer));
    monitoringManager->Start();

    auto orchidFactory = NYTree::GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();
    SyncYPathSetNode(
        ~orchidRoot,
        "monitoring",
        ~NYTree::CreateVirtualNode(~CreateMonitoringProducer(~monitoringManager)));
    SyncYPathSetNode(
        ~orchidRoot,
        "profiling",
        ~CreateVirtualNode(
            ~TProfilingManager::Get()->GetRoot()
            ->Via(TProfilingManager::Get()->GetInvoker())));
    SyncYPathSetNode(
        ~orchidRoot,
        "config",
        ~NYTree::CreateVirtualNode(~NYTree::CreateYsonFileProducer(ConfigFileName)));
    SyncYPathSetNode(
        ~orchidRoot,
        "stored_chunks",
        ~NYTree::CreateVirtualNode(~CreateStoredChunkMapService(~ChunkStore)));
    SyncYPathSetNode(
        ~orchidRoot,
        "cached_chunks",
        ~NYTree::CreateVirtualNode(~CreateCachedChunkMapService(~ChunkCache)));

    auto orchidService = New<TOrchidService>(
        ~orchidRoot,
        controlQueue->GetInvoker());
    rpcServer->RegisterService(~orchidService);

    THolder<NHttp::TServer> httpServer(new NHttp::TServer(Config->MonitoringPort));
    httpServer->Register(
        "/orchid",
        ~NMonitoring::GetYPathHttpHandler(~orchidRoot->Via(controlQueue->GetInvoker())));

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);
    httpServer->Start();

    LOG_INFO("Listening for RPC requests on port %d", Config->RpcPort);
    rpcServer->Start();

    masterConnector->Start();

    Sleep(TDuration::Max());
}

TBootstrap::TConfigPtr TBootstrap::GetConfig() const
{
    return Config;
}

TIncarnationId TBootstrap::GetIncarnationId() const
{
    return IncarnationId;
}

TChunkStorePtr TBootstrap::GetChunkStore() const
{
    return ChunkStore;
}

TChunkCachePtr TBootstrap::GetChunkCache() const
{
    return ChunkCache;
}

TSessionManagerPtr TBootstrap::GetSessionManager() const
{
    return SessionManager;
}

TJobExecutorPtr TBootstrap::GetJobExecutor() const
{
    return JobExecutor;
}

IInvoker::TPtr TBootstrap::GetControlInvoker() const
{
    return ControlInvoker;
}

TBlockStore::TPtr TBootstrap::GetBlockStore()
{
    return BlockStore;
}

IBusServer::TPtr TBootstrap::GetBusServer() const
{
    return BusServer;
}

TPeerBlockTablePtr TBootstrap::GetPeerBlockTable() const
{
    return PeerBlockTable;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
