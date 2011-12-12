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

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;
using namespace NChunkServer;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkCache::TImpl
    : public TCacheBase<TChunkId, TCachedChunk>
{
public:
    typedef TCapacityLimitedCache<TChunkId, TCachedChunk> TBase;
    typedef TIntrusivePtr<TImpl> TPtr;

    TImpl(
        const TChunkHolderConfig& config,
        TLocation* location,
        TMasterConnector* masterConnector)
        : Config(config)
        , Location(location)
        , MasterConnector(masterConnector)
    { }

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
        if (~MasterConnector == NULL) {
            return ToFuture(TDownloadResult(EErrorCode::MasterError, "Master connector is not initialized"));
        }

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
    TChunkHolderConfig Config;
    TLocation::TPtr Location;
    TMasterConnector::TPtr MasterConnector;

    DEFINE_BYREF_RW_PROPERTY(TParamSignal<TCachedChunk*>, ChunkAdded);
    DEFINE_BYREF_RW_PROPERTY(TParamSignal<TCachedChunk*>, ChunkRemoved);

    virtual bool NeedTrim() const
    {
        // TODO:
        return false;
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
        { }

        void Start()
        {
            LOG_INFO("Requesting chunk location (ChunkId: %s)", ~ChunkId.ToString());
            TChunkServiceProxy proxy(~Owner->MasterConnector->GetChannel());
            proxy.SetTimeout(Owner->Config.MasterRpcTimeout);
            auto request = proxy.FindChunk();
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

        void OnChunkFound(TChunkServiceProxy::TRspFindChunk::TPtr response)
        {
            if (!response->IsOK()) {
                auto message = Sprintf("Error requesting chunk location\n%s", ~response->GetError().ToString());
                OnError(TError(EErrorCode::MasterError, message));
                return;
            }

            auto holderAddresses = FromProto<Stroka>(response->holderaddresses());
            if (holderAddresses.empty()) {
                OnError(TError(EErrorCode::NotAvailable, "Chunk is not available"));
                return;
            }

            LOG_INFO("Chunk is found (ChunkId: %s, HolderAddresses: [%s])",
                ~ChunkId.ToString(),
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
                Owner->Config.CacheRemoteReader,
                ChunkId,
                holderAddresses);

            LOG_INFO("Getting chunk info from holders (ChunkId: %s)", ~ChunkId.ToString());
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

            LOG_INFO("Chunk info received from holders (ChunkId: %s)", ~ChunkId.ToString());
            ChunkInfo = result.Value();

            // Download all blocks.
            BlockCount = static_cast<int>(ChunkInfo.blocks_size());
            yvector<int> blockIndexes;
            blockIndexes.reserve(BlockCount);
            for (int index = 0; index < BlockCount; ++index) {
                blockIndexes.push_back(index);
            }

            SequentialReader = New<TSequentialReader>(
                Owner->Config.CacheSequentialReader,
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

            LOG_INFO("Asking for another block (ChunkId: %s, BlockIndex: %d)",
                ~ChunkId.ToString(),
                BlockIndex);

            SequentialReader->AsyncNextBlock()->Subscribe(
                FromMethod(&TThis::OnNextBlock, TPtr(this))
                ->Via(Invoker));
        }

        void OnNextBlock(TError error)
        {
            if (!error.IsOK()) {
                OnError(TError(EErrorCode::HolderError, error));
                return;
            }

            LOG_INFO("Block is received (ChunkId: %s)", ~ChunkId.ToString());

            LOG_INFO("Writing block (ChunkId: %s, BlockIndex: %d)",
                ~ChunkId.ToString(),
                BlockIndex);
            // NB: This is always done synchronously.
            auto writeResult = FileWriter->AsyncWriteBlock(SequentialReader->GetBlock())->Get();
            if (!writeResult.IsOK()) {
                OnError(writeResult);
                return;
            }
            LOG_INFO("Block is written (ChunkId: %s)", ~ChunkId.ToString());

            ++BlockIndex;
            FetchNextBlock();
        }

        void CloseChunk()
        {
            LOG_INFO("Closing chunk (ChunkId: %s)", ~ChunkId.ToString());
            // NB: This is always done synchronously.
            auto closeResult = FileWriter->AsyncClose(ChunkInfo.attributes())->Get();
            if (!closeResult.IsOK()) {
                OnError(closeResult);
                return;
            }
            LOG_INFO("Chunk is closed (ChunkId: %s)", ~ChunkId.ToString());

            OnSuccess();
        }

        void OnSuccess()
        {
            LOG_INFO("Chunk is downloaded into cache (ChunkId: %s)", ~ChunkId.ToString());
            auto chunk = New<TCachedChunk>(~Owner->Location, ChunkInfo);
            Cookie->EndInsert(chunk);
            Owner->Register(~chunk);
            Cleanup();
        }

        void OnError(const TError& error)
        {
            LOG_INFO("Error downloading chunk into cache (ChunkId: %s)\n%s",
                ~ChunkId.ToString(),
                ~error.ToString());
            Cleanup();
        }

        void Cleanup()
        {
            Owner.Reset();
            if (~FileWriter != NULL) {
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
    const TChunkHolderConfig& config,
    TReaderCache* readerCache,
    TMasterConnector* masterConnector)
{
    LOG_INFO("Chunk cache scan started");

    auto location = New<TLocation>(config.CacheLocation, readerCache);
    Impl = New<TImpl>(config, ~location, masterConnector);

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

DELEGATE_BYREF_RW_PROPERTY(TChunkCache, TParamSignal<TCachedChunk*>, ChunkAdded, *Impl);
DELEGATE_BYREF_RW_PROPERTY(TChunkCache, TParamSignal<TCachedChunk*>, ChunkRemoved, *Impl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
