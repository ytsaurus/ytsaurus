#include "stdafx.h"
#include "chunk_cache.h"
#include "private.h"
#include "reader_cache.h"
#include "location.h"
#include "chunk.h"
#include "block_store.h"
#include "config.h"
#include "master_connector.h"

#include <core/concurrency/thread_affinity.h>

#include <core/misc/serialize.h>
#include <core/misc/string.h>
#include <core/misc/fs.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/meta_state/master_channel.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/file_writer.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NCellNode;
using namespace NRpc;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkCache::TImpl
    : public TWeightLimitedCache<TChunkId, TCachedChunk>
{
public:
    typedef TWeightLimitedCache<TChunkId, TCachedChunk> TBase;

    TImpl(TDataNodeConfigPtr config, TBootstrap* bootstrap)
        : TBase(config->CacheLocation->Quota.Get(std::numeric_limits<i64>::max()))
        , Config(config)
        , Bootstrap(bootstrap)
    { }

    void Initialize()
    {
        LOG_INFO("Chunk cache scan started");

        Location = New<TLocation>(
            ELocationType::Cache,
            "cache",
            Config->CacheLocation,
            Bootstrap);

        Location->SubscribeDisabled(
            BIND(&TImpl::OnLocationDisabled, Unretained(this)));

        auto descriptors = Location->Initialize();
        for (const auto& descriptor : descriptors) {
            auto chunk = New<TCachedChunk>(
                Location,
                descriptor,
                Bootstrap->GetChunkCache(),
                Bootstrap->GetMemoryUsageTracker());
            Put(chunk);
        }

        LOG_INFO("Chunk cache scan completed, %d chunks found", GetSize());
    }

    void Register(TCachedChunkPtr chunk)
    {
        auto location = chunk->GetLocation();
        location->UpdateChunkCount(+1);
        location->UpdateUsedSpace(+chunk->GetInfo().disk_space());
    }

    void Unregister(TCachedChunkPtr chunk)
    {
        auto location = chunk->GetLocation();
        location->UpdateChunkCount(-1);
        location->UpdateUsedSpace(-chunk->GetInfo().disk_space());
    }

    void Put(TCachedChunkPtr chunk)
    {
        TInsertCookie cookie(chunk->GetId());
        YCHECK(BeginInsert(&cookie));
        cookie.EndInsert(chunk);
        Register(chunk);
    }

    TAsyncDownloadResult Download(
        const TChunkId& chunkId,
        TNodeDirectoryPtr nodeDirectory,
        const TChunkReplicaList& seedReplicas)
    {
        LOG_INFO("Getting chunk from cache (ChunkId: %s)",
            ~ToString(chunkId));

        std::shared_ptr<TInsertCookie> cookie = std::make_shared<TInsertCookie>(chunkId);
        if (BeginInsert(cookie.get())) {
            LOG_INFO("Loading chunk into cache (ChunkId: %s)", ~ToString(chunkId));
            auto session = New<TDownloadSession>(
                this,
                chunkId,
                nodeDirectory ? nodeDirectory : New<TNodeDirectory>(),
                seedReplicas,
                cookie);
            session->Start();
        } else {
            LOG_INFO("Chunk is already cached (ChunkId: %s)", ~ToString(chunkId));
        }

        return cookie->GetValue();
    }

    const TGuid& GetCellGuid()
    {
        return Location->GetCellGuid();
    }

    void UpdateCellGuid(const TGuid& cellGuid)
    {
        try {
            Location->SetCellGuid(cellGuid);
        } catch (const std::exception& ex) {
            Location->Disable();
            LOG_WARNING(ex, "Failed to set cell guid for chunk cache");
            return;
        }
    }

    bool IsEnabled() const
    {
        return Location->IsEnabled();
    }

private:
    TDataNodeConfigPtr Config;
    TBootstrap* Bootstrap;
    TLocationPtr Location;

    DEFINE_SIGNAL(void(TChunkPtr), ChunkAdded);
    DEFINE_SIGNAL(void(TChunkPtr), ChunkRemoved);

    virtual i64 GetWeight(TCachedChunk* chunk) const override
    {
        return chunk->GetInfo().disk_space();
    }

    virtual void OnAdded(TCachedChunk* value) override
    {
        TBase::OnAdded(value);
        ChunkAdded_.Fire(value);
    }

    virtual void OnRemoved(TCachedChunk* value) override
    {
        TBase::OnRemoved(value);
        ChunkRemoved_.Fire(value);
    }

    void OnLocationDisabled()
    {
        LOG_WARNING("Chunk cache disabled");
        Clear();

        // Register an alert and
        // schedule an out-of-order heartbeat to notify the master about the disaster.
        auto masterConnector = Bootstrap->GetMasterConnector();
        masterConnector->RegisterAlert("Chunk cache is disabled");
        masterConnector->ForceRegister();
    }


    class TDownloadSession
        : public TRefCounted
    {
    public:
        typedef TDownloadSession TThis;

        TDownloadSession(
            TImpl* owner,
            const TChunkId& chunkId,
            TNodeDirectoryPtr nodeDirectory,
            const TChunkReplicaList& seedReplicas,
            const std::shared_ptr<TInsertCookie>& cookie)
            : Owner(owner)
            , ChunkId(chunkId)
            , Cookie(cookie)
            , WriteInvoker(CreateSerializedInvoker(Owner->Location->GetWriteInvoker()))
            , NodeDirectory(nodeDirectory)
            , SeedReplicas(seedReplicas)
            , Logger(DataNodeLogger)
        {
            Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(ChunkId)));
        }

        void Start()
        {
            RemoteReader = CreateReplicationReader(
                Owner->Config->CacheRemoteReader,
                Owner->Bootstrap->GetBlockStore()->GetBlockCache(),
                Owner->Bootstrap->GetMasterChannel(),
                NodeDirectory,
                Owner->Bootstrap->GetLocalDescriptor(),
                ChunkId,
                SeedReplicas);

            WriteInvoker->Invoke(BIND(&TThis::DoStart, MakeStrong(this)));
        }

    private:
        TIntrusivePtr<TImpl> Owner;
        TChunkId ChunkId;
        std::vector<Stroka> SeedAddresses;
        std::shared_ptr<TInsertCookie> Cookie;
        IInvokerPtr WriteInvoker;
        TNodeDirectoryPtr NodeDirectory;
        TChunkReplicaList SeedReplicas;

        TFileWriterPtr FileWriter;
        IAsyncReaderPtr RemoteReader;
        TSequentialReaderPtr SequentialReader;
        TChunkMeta ChunkMeta;
        TChunkInfo ChunkInfo;
        int BlockCount;
        int BlockIndex;

        NLog::TTaggedLogger Logger;

        void DoStart()
        {
            Stroka fileName = Owner->Location->GetChunkFileName(ChunkId);
            try {
                NFS::ForcePath(NFS::GetDirectoryName(fileName));
                FileWriter = New<TFileWriter>(fileName);
                FileWriter->Open();
            } catch (const std::exception& ex) {
                LOG_FATAL(ex, "Error opening cached chunk for writing");
            }

            LOG_INFO("Getting chunk meta");
            RemoteReader->AsyncGetChunkMeta().Subscribe(
                BIND(&TThis::OnGotChunkMeta, MakeStrong(this))
                .Via(WriteInvoker));
        }

        void OnGotChunkMeta(IAsyncReader::TGetMetaResult result)
        {
            if (!result.IsOK()) {
                OnError(result);
                return;
            }

            LOG_INFO("Chunk meta received");
            ChunkMeta = result.Value();

            // Download all blocks.

            auto blocksExt = GetProtoExtension<TBlocksExt>(ChunkMeta.extensions());
            BlockCount = static_cast<int>(blocksExt.blocks_size());
            std::vector<TSequentialReader::TBlockInfo> blockSequence;
            blockSequence.reserve(BlockCount);
            for (int index = 0; index < BlockCount; ++index) {
                blockSequence.push_back(TSequentialReader::TBlockInfo(
                    index,
                    blocksExt.blocks(index).size()));
            }

            SequentialReader = New<TSequentialReader>(
                Owner->Config->CacheSequentialReader,
                std::move(blockSequence),
                RemoteReader,
                NCompression::ECodec::None);

            BlockIndex = 0;
            FetchNextBlock();
        }

        void FetchNextBlock()
        {
            if (BlockIndex >= BlockCount) {
                CloseChunk();
                return;
            }

            LOG_INFO("Asking for another block (BlockIndex: %d)",
                BlockIndex);

            SequentialReader->AsyncNextBlock().Subscribe(
                BIND(&TThis::OnNextBlock, MakeStrong(this))
                .Via(WriteInvoker));
        }

        void OnNextBlock(TError error)
        {
            if (!error.IsOK()) {
                OnError(error);
                return;
            }

            LOG_INFO("Writing block (BlockIndex: %d)", BlockIndex);
            // NB: This is always done synchronously.
            auto block = SequentialReader->GetBlock();
            if (!FileWriter->WriteBlock(block)) {
                OnError(FileWriter->GetReadyEvent().Get());
                return;
            }
            LOG_INFO("Block written");

            ++BlockIndex;
            FetchNextBlock();
        }

        void CloseChunk()
        {
            LOG_INFO("Closing chunk");
            // NB: This is always done synchronously.
            auto closeResult = FileWriter->AsyncClose(ChunkMeta).Get();

            if (!closeResult.IsOK()) {
                OnError(closeResult);
                return;
            }

            LOG_INFO("Chunk is closed");

            OnSuccess();
        }

        void OnSuccess()
        {
            LOG_INFO("Chunk is downloaded into cache");
            auto chunk = New<TCachedChunk>(
                Owner->Location,
                ChunkId,
                ChunkMeta,
                FileWriter->GetChunkInfo(),
                Owner->Bootstrap->GetChunkCache(),
                Owner->Bootstrap->GetMemoryUsageTracker());
            Cookie->EndInsert(chunk);
            Owner->Register(chunk);
            Cleanup();
        }

        void OnError(const TError& error)
        {
            YCHECK(!error.IsOK());
            auto wrappedError = TError("Error downloading chunk %s into cache", ~ToString(ChunkId))
                << error;
            Cookie->Cancel(wrappedError);
            LOG_WARNING(wrappedError);
            Cleanup();
        }

        void Cleanup()
        {
            Owner.Reset();
            if (FileWriter) {
                FileWriter.Reset();
            }
            RemoteReader.Reset();
            SequentialReader.Reset();
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

TChunkCache::TChunkCache(TDataNodeConfigPtr config, TBootstrap* bootstrap)
    : Impl(New<TImpl>(config, bootstrap))
{ }

void TChunkCache::Initialize()
{
    Impl->Initialize();
}

TChunkCache::~TChunkCache()
{ }

TCachedChunkPtr TChunkCache::FindChunk(const TChunkId& chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl->Find(chunkId);
}

TChunkCache::TChunks TChunkCache::GetChunks()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl->GetAll();
}

int TChunkCache::GetChunkCount()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl->GetSize();
}

TChunkCache::TAsyncDownloadResult TChunkCache::DownloadChunk(
    const TChunkId& chunkId,
    TNodeDirectoryPtr nodeDirectory,
    const TChunkReplicaList& seedReplicas)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl->Download(chunkId, nodeDirectory, seedReplicas);
}

const TGuid& TChunkCache::GetCellGuid() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl->GetCellGuid();
}

void TChunkCache::UpdateCellGuid(const TGuid& cellGuid)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl->UpdateCellGuid(cellGuid);
}

bool TChunkCache::IsEnabled() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl->IsEnabled();
}

DELEGATE_SIGNAL(TChunkCache, void(TChunkPtr), ChunkAdded, *Impl);
DELEGATE_SIGNAL(TChunkCache, void(TChunkPtr), ChunkRemoved, *Impl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
