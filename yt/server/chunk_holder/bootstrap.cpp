#include "stdafx.h"
#include "bootstrap.h"
#include "config.h"
#include "chunk.h"
#include "data_node_service.h"
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
#include "private.h"

#include <server/bootstrap/common.h>
#include <server/cell_node/bootstrap.h>

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/yson_string.h>
#include <ytlib/ytree/virtual.h>

#include <ytlib/rpc/server.h>
#include <ytlib/rpc/channel_cache.h>

namespace NYT {
namespace NChunkHolder {

using namespace NBus;
using namespace NRpc;
using namespace NChunkClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    TDataNodeConfigPtr config,
    NCellNode::TBootstrap* nodeBootstrap)
    : Config(config)
    , NodeBootstrap(nodeBootstrap)
{
    YCHECK(config);
    YCHECK(nodeBootstrap);
}

TBootstrap::~TBootstrap()
{ }

void TBootstrap::Init()
{
    ReaderCache = New<TReaderCache>(Config);

    ChunkRegistry = New<TChunkRegistry>(this);

    BlockStore = New<TBlockStore>(Config, this);

    PeerBlockTable = New<TPeerBlockTable>(Config->PeerBlockTable);

    PeerBlockUpdater = New<TPeerBlockUpdater>(Config, this);
    PeerBlockUpdater->Start();

    ChunkStore = New<TChunkStore>(Config, this);
    ChunkStore->Start();

    ChunkCache = New<TChunkCache>(Config, this);
    ChunkCache->Start();

    if (!ChunkStore->GetCellGuid().IsEmpty() && !ChunkCache->GetCellGuid().IsEmpty()) {
        LOG_FATAL_IF(
            ChunkStore->GetCellGuid().IsEmpty() != ChunkCache->GetCellGuid().IsEmpty(),
            "Inconsistent cell guid (ChunkStore: %s, ChunkCache: %s)",
            ~ChunkStore->GetCellGuid().ToString(),
            ~ChunkCache->GetCellGuid().ToString());
        CellGuid = ChunkCache->GetCellGuid();
    }

    if (!ChunkStore->GetCellGuid().IsEmpty() && ChunkCache->GetCellGuid().IsEmpty()) {
        CellGuid = ChunkStore->GetCellGuid();
        ChunkCache->UpdateCellGuid(CellGuid);
    } 
    
    if (ChunkStore->GetCellGuid().IsEmpty() && !ChunkCache->GetCellGuid().IsEmpty()) {
        CellGuid = ChunkCache->GetCellGuid();
        ChunkStore->SetCellGuid(CellGuid);
    }

    SessionManager = New<TSessionManager>(Config, this);

    JobExecutor = New<TJobExecutor>(this);

    MasterConnector = New<TMasterConnector>(Config, this);

    auto chunkHolderService = New<TDataNodeService>(Config, this);
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
    SetBuildAttributes(orchidRoot);

    MasterConnector->Start();
}

TDataNodeConfigPtr TBootstrap::GetConfig() const
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

const TGuid& TBootstrap::GetCellGuid() const
{
    return CellGuid;
}

void TBootstrap::UpdateCellGuid(const TGuid& cellGuid)
{
    CellGuid = cellGuid;
    ChunkStore->SetCellGuid(CellGuid);
    ChunkCache->UpdateCellGuid(CellGuid);
}

TMemoryUsageTracker<NCellNode::EMemoryConsumer>& TBootstrap::GetMemoryUsageTracker()
{
    return NodeBootstrap->GetMemoryUsageTracker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
