#include "stdafx.h"
#include "chunk_cache.h"
#include "common.h"
#include "reader_cache.h"
#include "location.h"
#include "chunk.h"
#include "block_store.h"
#include "config.h"
#include "bootstrap.h"
#include "chunk_meta_extensions.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/string.h>
#include <ytlib/misc/fs.h>
#include <ytlib/logging/tagged_logger.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/file_writer.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_server/chunk_service_proxy.h>
#include <ytlib/election/leader_channel.h>

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;
using namespace NChunkServer;
using namespace NElection;
using namespace NRpc;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChunkHolder");

////////////////////////////////////////////////////////////////////////////////

class TChunkCache::TImpl
    : public TWeightLimitedCache<TChunkId, TCachedChunk>
{
public:
    typedef TWeightLimitedCache<TChunkId, TCachedChunk> TBase;

    TImpl(TChunkHolderConfigPtr config, TBootstrap* bootstrap)
        : TBase(config->CacheLocation->Quota.Get(Max<i64>()))
        , Config(config)
        , Bootstrap(bootstrap)
    { }

    void Start()
    {
        LOG_INFO("Chunk cache scan started");

        Location = New<TLocation>(
            ELocationType::Cache,
            ~Config->CacheLocation,
            ~Bootstrap->GetReaderCache(),
            "ChunkCache");

        try {
            FOREACH (const auto& descriptor, Location->Scan()) {
                auto chunk = New<TCachedChunk>(
                    ~Location,
                    descriptor,
                    ~Bootstrap->GetChunkCache());
                Put(~chunk);
            }
        } catch (const std::exception& ex) {
            LOG_FATAL("Failed to initialize storage locations\n%s", ex.what());
        }

        LOG_INFO("Chunk cache scan completed, %d chunks found", GetSize());
    }

    void Register(TCachedChunkPtr chunk)
    {
        chunk->GetLocation()->UpdateUsedSpace(chunk->GetInfo().size());
    }

    void Unregister(TCachedChunkPtr chunk)
    {
        chunk->GetLocation()->UpdateUsedSpace(-chunk->GetInfo().size());
    }

    void Put(TCachedChunkPtr chunk)
    {
        TInsertCookie cookie(chunk->GetId());
        YVERIFY(BeginInsert(&cookie));
        cookie.EndInsert(chunk);
        Register(chunk);
    }

    TAsyncDownloadResult Download(
        const TChunkId& chunkId,
        const yvector<Stroka>& seedAddresses)
    {
        LOG_INFO("Getting chunk from cache (ChunkId: %s, SeedAddresses: [%s])",
            ~chunkId.ToString(),
            ~JoinToString(seedAddresses));

        TSharedPtr<TInsertCookie> cookie(new TInsertCookie(chunkId));
        if (BeginInsert(~cookie)) {
            LOG_INFO("Loading chunk into cache (ChunkId: %s)", ~chunkId.ToString());
            auto session = New<TDownloadSession>(this, chunkId, seedAddresses, cookie);
            session->Start();
        } else {
            LOG_INFO("Chunk is already cached (ChunkId: %s)", ~chunkId.ToString());
        }

        return cookie->GetValue();
    }

private:
    TChunkHolderConfigPtr Config;
    TBootstrap* Bootstrap;
    TLocationPtr Location;

    DEFINE_SIGNAL(void(TChunkPtr), ChunkAdded);
    DEFINE_SIGNAL(void(TChunkPtr), ChunkRemoved);

    virtual i64 GetWeight(TCachedChunk* chunk) const
    {
        return chunk->GetInfo().size();
    }

    virtual void OnAdded(TCachedChunk* value)
    {
        TBase::OnAdded(value);
        ChunkAdded_.Fire(value);
    }
    
    virtual void OnRemoved(TCachedChunk* value)
    {
        TBase::OnRemoved(value);
        ChunkRemoved_.Fire(value);
    }

    class TDownloadSession
        : public TRefCounted
    {
    public:
        typedef TDownloadSession TThis;

        TDownloadSession(
            TImpl* owner,
            const TChunkId& chunkId,
            const yvector<Stroka>& seedAddresses,
            TSharedPtr<TInsertCookie> cookie)
            : Owner(owner)
            , ChunkId(chunkId)
            , SeedAddresses(seedAddresses)
            , Cookie(cookie)
            , Invoker(Owner->Location->GetInvoker())
            , Logger(ChunkHolderLogger)
        {
            Logger.AddTag(Sprintf("ChunkId: %s", ~ChunkId.ToString()));
        }

        void Start()
        {
            Stroka fileName = Owner->Location->GetChunkFileName(ChunkId);
            try {
                NFS::ForcePath(NFS::GetDirectoryName(fileName));
                FileWriter = New<TFileWriter>(fileName);
                FileWriter->Open();
            } catch (const std::exception& ex) {
                LOG_FATAL("Error opening cached chunk for writing\n%s", ex.what());
            }

            RemoteReader = CreateRemoteReader(
                ~Owner->Config->CacheRemoteReader,
                ~Owner->Bootstrap->GetBlockStore()->GetBlockCache(),
                ~Owner->Bootstrap->GetMasterChannel(),
                ChunkId,
                SeedAddresses);

            LOG_INFO("Getting chunk info from holders");
            RemoteReader->AsyncGetChunkMeta().Subscribe(
                BIND(&TThis::OnGotChunkMeta, MakeStrong(this))
                .Via(Invoker));
        }

    private:
        TIntrusivePtr<TImpl> Owner;
        TChunkId ChunkId;
        yvector<Stroka> SeedAddresses;
        TSharedPtr<TInsertCookie> Cookie;
        IInvoker::TPtr Invoker;

        TFileWriter::TPtr FileWriter;
        IAsyncReaderPtr RemoteReader;
        TSequentialReaderPtr SequentialReader;
        TChunkMeta ChunkMeta;
        TChunkInfo ChunkInfo;
        int BlockCount;
        int BlockIndex;

        NLog::TTaggedLogger Logger;

        void OnGotChunkMeta(IAsyncReader::TGetMetaResult result)
        {
            if (!result.IsOK()) {
                OnError(result);
                return;
            }

            LOG_INFO("Chunk info received from holders");
            ChunkMeta = result.Value();

            // Download all blocks.

            auto blocksExtension = GetProtoExtension<TBlocksExt>(ChunkMeta.extensions());
            BlockCount = static_cast<int>(blocksExtension->blocks_size());
            yvector<int> blockIndexes;
            blockIndexes.reserve(BlockCount);
            for (int index = 0; index < BlockCount; ++index) {
                blockIndexes.push_back(index);
            }

            SequentialReader = New<TSequentialReader>(
                ~Owner->Config->CacheSequentialReader,
                blockIndexes,
                ~RemoteReader,
                blocksExtension);

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
                .Via(Invoker));
        }

        void OnNextBlock(TError error)
        {
            if (!error.IsOK()) {
                OnError(error);
                return;
            }

            LOG_INFO("Writing block (BlockIndex: %d)", BlockIndex);
            // NB: This is always done synchronously.
            std::vector<TSharedRef> blocks;
            blocks.push_back(SequentialReader->GetBlock());
            auto writeResult = FileWriter->AsyncWriteBlocks(MoveRV(blocks)).Get();
            if (!writeResult.IsOK()) {
                OnError(writeResult);
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
                ~Owner->Location,
                ChunkId,
                ChunkMeta,
                FileWriter->GetChunkInfo(),
                ~Owner->Bootstrap->GetChunkCache());
            Cookie->EndInsert(chunk);
            Owner->Register(~chunk);
            Cleanup();
        }

        void OnError(const TError& error)
        {
            YASSERT(!error.IsOK());
            TError wrappedError(
                error.GetCode(),
                Sprintf("Error downloading chunk into cache (ChunkId: %s)\n%s",
                    ~ChunkId.ToString(),
                    ~error.ToString()));
            Cookie->Cancel(wrappedError);
            LOG_WARNING("%s", ~wrappedError.ToString());
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

TChunkCache::TChunkCache(TChunkHolderConfigPtr config, TBootstrap* bootstrap)
    : Impl(New<TImpl>(config, bootstrap))
{ }

void TChunkCache::Start()
{
    Impl->Start();
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
    const yvector<Stroka>& seedAddresses)
{
    VERIFY_THREAD_AFFINITY_ANY();
    return Impl->Download(chunkId, seedAddresses);
}

DELEGATE_SIGNAL(TChunkCache, void(TChunkPtr), ChunkAdded, *Impl);
DELEGATE_SIGNAL(TChunkCache, void(TChunkPtr), ChunkRemoved, *Impl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
