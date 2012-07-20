#include "stdafx.h"
#include "bootstrap.h"
#include "config.h"
#include "chunk.h"
#include "chunk_holder_service.h"
#include "reader_cache.h"
#include "session_manager.h"
#include "block_store.h"
#include "peer_block_table.h"
#include "chunk_store.h"
#include "chunk_cache.h"
#include "location.h"
#include "chunk_registry.h"
#include "master_connector.h"
#include "job_executor.h"
#include "peer_block_updater.h"
#include "ytree_integration.h"

#include <ytlib/cell_node/bootstrap.h>

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/yson_string.h>
#include <ytlib/ytree/virtual.h>

#include <ytlib/rpc/server.h>
#include <ytlib/rpc/channel_cache.h>

#include <build.h>

namespace NYT {
namespace NChunkHolder {

using namespace NBus;
using namespace NRpc;
using namespace NChunkServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    TChunkHolderConfigPtr config,
    NCellNode::TBootstrap* nodeBootstrap)
    : Config(config)
    , NodeBootstrap(nodeBootstrap)
{
    YASSERT(config);
    YASSERT(nodeBootstrap);
}

TBootstrap::~TBootstrap()
{ }

void TBootstrap::Init()
{
    ReaderCache = New<TReaderCache>(Config);

    ReadRouterQueue = New<TActionQueue>("ReadRouter");
    ReadThreadPool = New<TThreadPool>(Config->ReadPoolThreadCount, "ReadPool");

    WriteRouterQueue = New<TActionQueue>("WriteRouter");
    WriteThreadPool = New<TThreadPool>(Config->WritePoolThreadCount, "WritePool");

    ChunkRegistry = New<TChunkRegistry>(this);

    BlockStore = New<TBlockStore>(Config, this);

    PeerBlockTable = New<TPeerBlockTable>(Config->PeerBlockTable);

    PeerBlockUpdater = New<TPeerBlockUpdater>(Config, this);
    PeerBlockUpdater->Start();

    ChunkStore = New<TChunkStore>(Config, this);
    ChunkStore->Start();

    ChunkCache = New<TChunkCache>(Config, this);
    ChunkCache->Start();

    SessionManager = New<TSessionManager>(Config, this);

    JobExecutor = New<TJobExecutor>(
        Config,
        ChunkStore,
        BlockStore,
        NodeBootstrap->GetControlInvoker());

    MasterConnector = New<TMasterConnector>(Config, this);

    auto chunkHolderService = New<TChunkHolderService>(Config, this);
    NodeBootstrap->GetRpcServer()->RegisterService(chunkHolderService);

    auto orchidRoot = NodeBootstrap->GetOrchidRoot();

    SetNodeByYPath(
        orchidRoot,
        "/stored_chunks",
        CreateVirtualNode(CreateStoredChunkMapService(~ChunkStore)));
    SetNodeByYPath(
        orchidRoot,
        "/cached_chunks",
        CreateVirtualNode(CreateCachedChunkMapService(~ChunkCache)));
    SyncYPathSet(orchidRoot, "/@service_name", ConvertToYsonString("node"));

    SyncYPathSet(orchidRoot, "/@version", ConvertToYsonString(YT_VERSION));
    SyncYPathSet(orchidRoot, "/@build_host", ConvertToYsonString(YT_BUILD_HOST));
    SyncYPathSet(orchidRoot, "/@build_time", ConvertToYsonString(YT_BUILD_TIME));
    SyncYPathSet(orchidRoot, "/@build_machine", ConvertToYsonString(YT_BUILD_MACHINE));

    MasterConnector->Start();
}

TChunkHolderConfigPtr TBootstrap::GetConfig() const
{
    return Config;
}

TIncarnationId TBootstrap::GetIncarnationId() const
{
    return NodeBootstrap->GetIncarnationId();
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

IInvokerPtr TBootstrap::GetControlInvoker() const
{
    return NodeBootstrap->GetControlInvoker();
}

IInvokerPtr TBootstrap::GetReadRouterInvoker() const
{
    return ReadRouterQueue->GetInvoker();
}

IInvokerPtr TBootstrap::GetReadPoolInvoker() const
{
    return ReadThreadPool->GetInvoker();
}

IInvokerPtr TBootstrap::GetWriteRouterInvoker() const
{
    return WriteRouterQueue->GetInvoker();
}

IInvokerPtr TBootstrap::GetWritePoolInvoker() const
{
    return WriteThreadPool->GetInvoker();
}

TChunkRegistryPtr TBootstrap::GetChunkRegistry() const
{
    return ChunkRegistry;
}

TBlockStorePtr TBootstrap::GetBlockStore()
{
    return BlockStore;
}

TPeerBlockTablePtr TBootstrap::GetPeerBlockTable() const
{
    return PeerBlockTable;
}

TReaderCachePtr TBootstrap::GetReaderCache() const
{
    return ReaderCache;
}

IChannelPtr TBootstrap::GetMasterChannel() const
{
    return NodeBootstrap->GetMasterChannel();
}

Stroka TBootstrap::GetPeerAddress() const
{
    return NodeBootstrap->GetPeerAddress();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
