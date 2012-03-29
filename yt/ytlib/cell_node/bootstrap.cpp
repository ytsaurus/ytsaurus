#include "stdafx.h"
#include "bootstrap.h"
#include "config.h"

#include <ytlib/misc/ref_counted_tracker.h>
#include <ytlib/bus/nl_server.h>
#include <ytlib/rpc/channel_cache.h>
#include <ytlib/election/leader_channel.h>
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
#include <ytlib/chunk_holder/config.h>
#include <ytlib/chunk_holder/chunk_holder_service.h>
#include <ytlib/chunk_holder/reader_cache.h>
#include <ytlib/chunk_holder/session_manager.h>
#include <ytlib/chunk_holder/block_store.h>
#include <ytlib/chunk_holder/peer_block_table.h>
#include <ytlib/chunk_holder/chunk_store.h>
#include <ytlib/chunk_holder/chunk_cache.h>
#include <ytlib/chunk_holder/chunk_registry.h>
#include <ytlib/chunk_holder/master_connector.h>
#include <ytlib/chunk_holder/job_executor.h>
#include <ytlib/chunk_holder/peer_block_updater.h>
#include <ytlib/chunk_holder/ytree_integration.h>

namespace NYT {
namespace NCellNode {

using namespace NBus;
using namespace NRpc;
using namespace NYTree;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NChunkServer;
using namespace NProfiling;
using namespace NChunkHolder;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChunkHolder");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    const Stroka& configFileName,
    TCellNodeConfigPtr config)
    : ConfigFileName(configFileName)
    , Config(config)
{ }

TBootstrap::~TBootstrap()
{ }

void TBootstrap::Run()
{
    IncarnationId = TIncarnationId::Create();
    PeerAddress = Sprintf("%s:%d", GetHostName(), Config->RpcPort);

    LOG_INFO("Starting chunk holder (IncarnationId: %s, PeerAddress: %s, MasterAddresses: [%s])",
        ~IncarnationId.ToString(),
        ~PeerAddress,
        ~JoinToString(Config->Masters->Addresses));

    LeaderChannel = CreateLeaderChannel(~Config->Masters);

    auto controlQueue = New<TActionQueue>("Control");
    ControlInvoker = controlQueue->GetInvoker();

    BusServer = CreateNLBusServer(~New<TNLBusServerConfig>(Config->RpcPort));

    auto rpcServer = CreateRpcServer(~BusServer);

    ReaderCache = New<TReaderCache>(Config->ChunkHolder);

    auto chunkRegistry = New<TChunkRegistry>(this);

    BlockStore = New<TBlockStore>(
        Config->ChunkHolder,
        chunkRegistry,
        ReaderCache);

    PeerBlockTable = New<TPeerBlockTable>(Config->ChunkHolder->PeerBlockTable);

    auto peerUpdater = New<TPeerBlockUpdater>(Config->ChunkHolder, this);
    peerUpdater->Start();

    ChunkStore = New<TChunkStore>(Config->ChunkHolder, this);
    ChunkStore->Start();

    ChunkCache = New<TChunkCache>(Config->ChunkHolder, this);
    ChunkCache->Start();

    SessionManager = New<TSessionManager>(
        Config->ChunkHolder,
        BlockStore,
        ChunkStore,
        controlQueue->GetInvoker());

    JobExecutor = New<TJobExecutor>(
        Config->ChunkHolder,
        ChunkStore,
        BlockStore,
        controlQueue->GetInvoker());

    auto masterConnector = New<TMasterConnector>(Config->ChunkHolder, this);

    auto chunkHolderService = New<TChunkHolderService>(Config->ChunkHolder, this);
    rpcServer->RegisterService(~chunkHolderService);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        FromMethod(&TRefCountedTracker::GetMonitoringInfo, TRefCountedTracker::Get()));
    monitoringManager->Register(
        "/bus_server",
        FromMethod(&IBusServer::GetMonitoringInfo, BusServer));
    monitoringManager->Start();

    auto orchidFactory = NYTree::GetEphemeralNodeFactory();
    auto orchidRoot = orchidFactory->CreateMap();
    SyncYPathSetNode(
        ~orchidRoot,
        "/monitoring",
        ~NYTree::CreateVirtualNode(~CreateMonitoringProducer(~monitoringManager)));
    SyncYPathSetNode(
        ~orchidRoot,
        "/profiling",
        ~CreateVirtualNode(
            ~TProfilingManager::Get()->GetRoot()
            ->Via(TProfilingManager::Get()->GetInvoker())));
    SyncYPathSetNode(
        ~orchidRoot,
        "/config",
        ~NYTree::CreateVirtualNode(~NYTree::CreateYsonFileProducer(ConfigFileName)));
    SyncYPathSetNode(
        ~orchidRoot,
        "/stored_chunks",
        ~NYTree::CreateVirtualNode(~CreateStoredChunkMapService(~ChunkStore)));
    SyncYPathSetNode(
        ~orchidRoot,
        "/cached_chunks",
        ~NYTree::CreateVirtualNode(~CreateCachedChunkMapService(~ChunkCache)));

    auto orchidService = New<TOrchidService>(
        ~orchidRoot,
        controlQueue->GetInvoker());
    rpcServer->RegisterService(~orchidService);

    ::THolder<NHttp::TServer> httpServer(new NHttp::TServer(Config->MonitoringPort));
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

TCellNodeConfigPtr TBootstrap::GetConfig() const
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

TBlockStorePtr TBootstrap::GetBlockStore()
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

TReaderCachePtr TBootstrap::GetReaderCache() const
{
    return ReaderCache;
}

IChannel::TPtr TBootstrap::GetLeaderChannel() const
{
    return LeaderChannel;
}

Stroka TBootstrap::GetPeerAddress() const
{
    return PeerAddress;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
