#include "stdafx.h"
#include "chunk_cache.h"
#include "location.h"

#include "../misc/thread_affinity.h"
#include "../misc/serialize.h"
#include "../misc/string.h"
#include "../chunk_client/file_writer.h"
#include "../chunk_client/remote_reader.h"
#include "../chunk_client/sequential_reader.h"
#include "../chunk_server/chunk_service_rpc.h"
#include "../election/cell_channel.h"
#include "../logging/tagged_logger.h"

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;
using namespace NChunkServer;
using namespace NElection;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkCache::TImpl
    : public TWeightLimitedCache<TChunkId, TCachedChunk>
{
public:
    typedef TWeightLimitedCache<TChunkId, TCachedChunk> TBase;
    typedef TIntrusivePtr<TImpl> TPtr;

    TImpl(
        TChunkHolderConfig* config,
        TLocation* location)
        : TBase(config->ChunkCacheLocation->Quota == 0 ? Max<i64>() : config->ChunkCacheLocation->Quota)
        , Config(config)
        , Location(location)
    {
        auto channel = CreateCellChannel(~config->Masters);
        ChunkProxy = new TChunkServiceProxy (~channel);
        ChunkProxy->SetTimeout(config->MasterRpcTimeout);
    }

    void Register(TCachedChunk* chunk)
    {
        chunk->GetLocation()->RegisterChunk(chunk);
    }

    void Unregister(TCachedChunk* chunk)
    {
        chunk->GetLocation()->UnregisterChunk(chunk);
    }

    void Put(TCachedChunk* chunk)
    {
        TInsertCookie cookie(chunk->GetId());
        YVERIFY(BeginInsert(&cookie));
        cookie.EndInsert(chunk);
        Register(chunk);
    }

    TAsyncDownloadResult::TPtr Download(const TChunkId& chunkId)
    {
        LOG_INFO("Getting chunk from cache (ChunkId: %s)", ~chunkId.ToString());

        TSharedPtr<TInsertCookie> cookie(new TInsertCookie(chunkId));
        if (BeginInsert(~cookie)) {
            LOG_INFO("Loading chunk into cache (ChunkId: %s)", ~chunkId.ToString());
            auto session = New<TDownloadSession>(this, chunkId, cookie);
            session->Start();
        } else {
            LOG_INFO("Chunk is already cached (ChunkId: %s)", ~chunkId.ToString());
        }

        return cookie->GetAsyncResult();
    }

private:
    TChunkHolderConfig::TPtr Config;
    TLocation::TPtr Location;
    TAutoPtr<TChunkServiceProxy> ChunkProxy;

    DEFINE_BYREF_RW_PROPERTY(TParamSignal<TChunk*>, ChunkAdded);
    DEFINE_BYREF_RW_PROPERTY(TParamSignal<TChunk*>, ChunkRemoved);

    virtual i64 GetWeight(TCachedChunk* chunk) const
    {
        return chunk->GetSize();
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
        : public TRefCountedBase
    {
    public:
        typedef TDownloadSession TThis;
        typedef TIntrusivePtr<TThis> TPtr;

        TDownloadSession(
            TImpl* owner,
            const TChunkId& chunkId,
            TSharedPtr<TInsertCookie> cookie)
            : Owner(owner)
            , ChunkId(chunkId)
            , Cookie(cookie)
            , Invoker(Owner->Location->GetInvoker())
            , Logger(ChunkHolderLogger)
        {
            Logger.SetTag(Sprintf("ChunkId: %s", ~ChunkId.ToString()));
        }

        void Start()
        {
            LOG_INFO("Requesting chunk location");
            auto request = Owner->ChunkProxy->FindChunk();
            request->set_chunkid(ChunkId.ToProto());
            request->Invoke()->Subscribe(
                FromMethod(&TThis::OnChunkFound, TPtr(this))
                ->Via(Invoker));
        }

    private:
        TImpl::TPtr Owner;
        TChunkId ChunkId;
        TSharedPtr<TInsertCookie> Cookie;
        IInvoker::TPtr Invoker;

        TChunkFileWriter::TPtr FileWriter;
        TRemoteReader::TPtr RemoteReader;
        TSequentialReader::TPtr SequentialReader;
        TChunkInfo ChunkInfo;
        int BlockCount;
        int BlockIndex;

        NLog::TTaggedLogger Logger;

        void OnChunkFound(TChunkServiceProxy::TRspFindChunk::TPtr response)
        {
            if (!response->IsOK()) {
                auto message = Sprintf("Error requesting chunk location\n%s", ~response->GetError().ToString());
                OnError(TError(EErrorCode::MasterError, message));
                return;
            }

            auto holderAddresses = FromProto<Stroka>(response->holderaddresses());
            if (holderAddresses.empty()) {
                OnError(TError(EErrorCode::ChunkLost, "Chunk is lost"));
                return;
            }

            LOG_INFO("Chunk is found (HolderAddresses: [%s])",
                ~JoinToString(holderAddresses));

            Stroka fileName = Owner->Location->GetChunkFileName(ChunkId);
            try {
                NFS::ForcePath(NFS::GetDirectoryName(fileName));
                FileWriter = New<TChunkFileWriter>(ChunkId, fileName);
            } catch (...) {
                LOG_FATAL("Error opening cached chunk for writing\n%s", ~CurrentExceptionMessage());
            }

            // ToDo: use TRetriableReader.
            RemoteReader = New<TRemoteReader>(
                ~Owner->Config->CacheRemoteReader,
                ChunkId,
                holderAddresses);

            LOG_INFO("Getting chunk info from holders");
            RemoteReader->AsyncGetChunkInfo()->Subscribe(
                FromMethod(&TThis::OnGotChunkInfo, TPtr(this))
                ->Via(Invoker));
        }

        void OnGotChunkInfo(IAsyncReader::TGetInfoResult result)
        {
            if (!result.IsOK()) {
                auto message = Sprintf("Error requesting chunk info from holders\n%s", ~result.ToString());
                OnError(TError(EErrorCode::HolderError, message));
                return;
            }

            LOG_INFO("Chunk info received from holders");
            ChunkInfo = result.Value();

            // Download all blocks.
            BlockCount = static_cast<int>(ChunkInfo.blocks_size());
            yvector<int> blockIndexes;
            blockIndexes.reserve(BlockCount);
            for (int index = 0; index < BlockCount; ++index) {
                blockIndexes.push_back(index);
            }

            SequentialReader = New<TSequentialReader>(
                ~Owner->Config->CacheSequentialReader,
                blockIndexes,
                ~RemoteReader);

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

            SequentialReader->AsyncNextBlock()->Subscribe(
                FromMethod(&TThis::OnNextBlock, TPtr(this))
                ->Via(Invoker));
        }

        void OnNextBlock(TError error)
        {
            if (!error.IsOK()) {
                OnError(TError(EErrorCode::HolderError, error.ToString()));
                return;
            }

            LOG_INFO("Block is received");

            LOG_INFO("Writing block (BlockIndex: %d)",
                BlockIndex);
            // NB: This is always done synchronously.
            auto writeResult = FileWriter->AsyncWriteBlock(SequentialReader->GetBlock())->Get();
            if (!writeResult.IsOK()) {
                OnError(writeResult);
                return;
            }
            LOG_INFO("Block is written");

            ++BlockIndex;
            FetchNextBlock();
        }

        void CloseChunk()
        {
            LOG_INFO("Closing chunk");
            // NB: This is always done synchronously.
            auto closeResult = FileWriter->AsyncClose(ChunkInfo.attributes())->Get();
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
            auto chunk = New<TCachedChunk>(~Owner->Location, ChunkInfo);
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
                FileWriter->Cancel(TError("Chunk download canceled"));
                FileWriter.Reset();
            }
            RemoteReader.Reset();
            SequentialReader.Reset();
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

TChunkCache::TChunkCache(
    TChunkHolderConfig* config,
    TReaderCache* readerCache)
{
    LOG_INFO("Chunk cache scan started");

    auto location = New<TLocation>(
        ELocationType::Cache,
        ~config->ChunkCacheLocation,
        readerCache,
        "ChunkCache");
    Impl = New<TImpl>(config, ~location);

    try {
        FOREACH (const auto& descriptor, location->Scan()) {
            auto chunk = New<TCachedChunk>(~location, descriptor);
            Impl->Put(~chunk);
        }
    } catch (...) {
        LOG_FATAL("Failed to initialize storage locations\n%s", ~CurrentExceptionMessage());
    }

    LOG_INFO("Chunk cache scan completed, %d chunk(s) total", GetChunkCount());
}

TCachedChunk::TPtr TChunkCache::FindChunk(const TChunkId& chunkId)
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

TChunkCache::TAsyncDownloadResult::TPtr TChunkCache::DownloadChunk(const TChunkId& chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();
    return Impl->Download(chunkId);
}

DELEGATE_BYREF_RW_PROPERTY(TChunkCache, TParamSignal<TChunk*>, ChunkAdded, *Impl);
DELEGATE_BYREF_RW_PROPERTY(TChunkCache, TParamSignal<TChunk*>, ChunkRemoved, *Impl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
