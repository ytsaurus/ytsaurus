#include "stdafx.h"
#include "chunk_cache.h"
#include "private.h"
#include "reader_cache.h"
#include "location.h"
#include "blob_chunk.h"
#include "block_store.h"
#include "config.h"
#include "master_connector.h"

#include <core/concurrency/thread_affinity.h>

#include <core/misc/serialize.h>
#include <core/misc/string.h>
#include <core/misc/fs.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/hydra/peer_channel.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/file_writer.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/sequential_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/api/client.h>

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
    : public TWeightLimitedCache<TChunkId, TCachedBlobChunk>
{
public:
    TImpl(TDataNodeConfigPtr config, TBootstrap* bootstrap)
        : TWeightLimitedCache(config->CacheLocation->Quota.Get(std::numeric_limits<i64>::max()))
        , Config_(config)
        , Bootstrap_(bootstrap)
    { }

    void Initialize()
    {
        LOG_INFO("Chunk cache scan started");

        Location_ = New<TLocation>(
            ELocationType::Cache,
            "cache",
            Config_->CacheLocation,
            Bootstrap_);

        Location_->SubscribeDisabled(
            BIND(&TImpl::OnLocationDisabled, Unretained(this)));

        auto descriptors = Location_->Initialize();
        for (const auto& descriptor : descriptors) {
            auto chunk = New<TCachedBlobChunk>(
                Location_,
                descriptor,
                Bootstrap_->GetChunkCache(),
                &Bootstrap_->GetMemoryUsageTracker());
            Put(chunk);
        }

        LOG_INFO("Chunk cache scan completed, %d chunks found", GetSize());
    }

    void Register(TCachedBlobChunkPtr chunk)
    {
        auto location = chunk->GetLocation();
        location->UpdateChunkCount(+1);
        location->UpdateUsedSpace(+chunk->GetInfo().disk_space());
    }

    void Unregister(TCachedBlobChunkPtr chunk)
    {
        auto location = chunk->GetLocation();
        location->UpdateChunkCount(-1);
        location->UpdateUsedSpace(-chunk->GetInfo().disk_space());
    }

    void Put(TCachedBlobChunkPtr chunk)
    {
        TInsertCookie cookie(chunk->GetId());
        YCHECK(BeginInsert(&cookie));
        cookie.EndInsert(chunk);
        Register(chunk);
    }

    TAsyncDownloadResult Download(const TChunkId& chunkId)
    {
        LOG_INFO("Getting chunk from cache (ChunkId: %s)",
            ~ToString(chunkId));

        TInsertCookie cookie(chunkId);
        if (BeginInsert(&cookie)) {
            LOG_INFO("Loading chunk into cache (ChunkId: %s)", ~ToString(chunkId));
            auto session = New<TDownloadSession>(this, chunkId, std::move(cookie));
            session->Start();
        } else {
            LOG_INFO("Chunk is already cached (ChunkId: %s)", ~ToString(chunkId));
        }

        return cookie.GetValue().Apply(BIND([] (TErrorOr<TCachedBlobChunkPtr> result) -> TDownloadResult {
            return result.IsOK() ? TDownloadResult(result.Value()) : TDownloadResult(result);
        }));
    }

    bool IsEnabled() const
    {
        return Location_->IsEnabled();
    }

    std::vector<IChunkPtr> GetChunks()
    {
        auto chunks = GetAll();
        return std::vector<IChunkPtr>(chunks.begin(), chunks.end());
    }

private:
    TDataNodeConfigPtr Config_;
    TBootstrap* Bootstrap_;
    TLocationPtr Location_;

    DEFINE_SIGNAL(void(IChunkPtr), ChunkAdded);
    DEFINE_SIGNAL(void(IChunkPtr), ChunkRemoved);


    virtual i64 GetWeight(TCachedBlobChunk* chunk) const override
    {
        return chunk->GetInfo().disk_space();
    }

    virtual void OnAdded(TCachedBlobChunk* value) override
    {
        TWeightLimitedCache::OnAdded(value);
        ChunkAdded_.Fire(value);
    }

    virtual void OnRemoved(TCachedBlobChunk* value) override
    {
        TWeightLimitedCache::OnRemoved(value);
        ChunkRemoved_.Fire(value);
    }

    void OnLocationDisabled()
    {
        LOG_WARNING("Chunk cache disabled");
        Clear();

        // Register an alert and
        // schedule an out-of-order heartbeat to notify the master about the disaster.
        auto masterConnector = Bootstrap_->GetMasterConnector();
        masterConnector->RegisterAlert("Chunk cache is disabled");
        masterConnector->ForceRegister();
    }


    class TDownloadSession
        : public TRefCounted
    {
    public:
        TDownloadSession(
            TImpl* owner,
            const TChunkId& chunkId,
            TInsertCookie cookie)
            : Owner_(owner)
            , ChunkId_(chunkId)
            , Cookie_(std::move(cookie))
            , WriteInvoker_(CreateSerializedInvoker(Owner_->Location_->GetWriteInvoker()))
            , NodeDirectory_(New<TNodeDirectory>())
            , Logger(DataNodeLogger)
        {
            Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(ChunkId_)));
        }

        void Start()
        {
            RemoteReader_ = CreateReplicationReader(
                Owner_->Config_->CacheRemoteReader,
                Owner_->Bootstrap_->GetBlockStore()->GetBlockCache(),
                Owner_->Bootstrap_->GetMasterClient()->GetMasterChannel(),
                NodeDirectory_,
                Owner_->Bootstrap_->GetLocalDescriptor(),
                ChunkId_);

            WriteInvoker_->Invoke(BIND(&TDownloadSession::DoStart, MakeStrong(this)));
        }

    private:
        TIntrusivePtr<TImpl> Owner_;
        TChunkId ChunkId_;
        std::vector<Stroka> SeedAddresses_;
        TInsertCookie Cookie_;
        IInvokerPtr WriteInvoker_;
        TNodeDirectoryPtr NodeDirectory_;

        TFileWriterPtr FileWriter_;
        IReaderPtr RemoteReader_;
        TSequentialReaderPtr SequentialReader_;
        TChunkMeta ChunkMeta_;
        TChunkInfo ChunkInfo_;
        int BlockCount_;
        int BlockIndex_;

        NLog::TTaggedLogger Logger;


        void DoStart()
        {
            Stroka fileName = Owner_->Location_->GetChunkFileName(ChunkId_);
            try {
                NFS::ForcePath(NFS::GetDirectoryName(fileName));
                FileWriter_ = New<TFileWriter>(fileName);
                FileWriter_->Open();
            } catch (const std::exception& ex) {
                LOG_FATAL(ex, "Error opening cached chunk for writing");
            }

            LOG_INFO("Getting chunk meta");
            RemoteReader_->GetChunkMeta().Subscribe(
                BIND(&TDownloadSession::OnGotChunkMeta, MakeStrong(this))
                .Via(WriteInvoker_));
        }

        void OnGotChunkMeta(IReader::TGetMetaResult result)
        {
            if (!result.IsOK()) {
                OnError(result);
                return;
            }

            LOG_INFO("Chunk meta received");
            ChunkMeta_ = result.Value();

            // Download all blocks.

            auto blocksExt = GetProtoExtension<TBlocksExt>(ChunkMeta_.extensions());
            BlockCount_ = static_cast<int>(blocksExt.blocks_size());
            std::vector<TSequentialReader::TBlockInfo> blockSequence;
            blockSequence.reserve(BlockCount_);
            for (int index = 0; index < BlockCount_; ++index) {
                blockSequence.push_back(TSequentialReader::TBlockInfo(
                    index,
                    blocksExt.blocks(index).size()));
            }

            SequentialReader_ = New<TSequentialReader>(
                Owner_->Config_->CacheSequentialReader,
                std::move(blockSequence),
                RemoteReader_,
                NCompression::ECodec::None);

            BlockIndex_ = 0;
            FetchNextBlock();
        }

        void FetchNextBlock()
        {
            if (BlockIndex_ >= BlockCount_) {
                CloseChunk();
                return;
            }

            LOG_INFO("Asking for another block (BlockIndex: %d)",
                BlockIndex_);

            SequentialReader_->AsyncNextBlock().Subscribe(
                BIND(&TDownloadSession::OnNextBlock, MakeStrong(this))
                    .Via(WriteInvoker_));
        }

        void OnNextBlock(TError error)
        {
            if (!error.IsOK()) {
                OnError(error);
                return;
            }

            LOG_INFO("Writing block (BlockIndex: %d)", BlockIndex_);
            // NB: This is always done synchronously.
            auto block = SequentialReader_->GetBlock();
            if (!FileWriter_->WriteBlock(block)) {
                OnError(FileWriter_->GetReadyEvent().Get());
                return;
            }
            LOG_INFO("Block written");

            ++BlockIndex_;
            FetchNextBlock();
        }

        void CloseChunk()
        {
            LOG_INFO("Closing chunk");
            // NB: This is always done synchronously.
            auto closeResult = FileWriter_->Close(ChunkMeta_).Get();
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
            auto chunk = New<TCachedBlobChunk>(
                Owner_->Location_,
                ChunkId_,
                ChunkMeta_,
                FileWriter_->GetChunkInfo(),
                Owner_->Bootstrap_->GetChunkCache(),
                &Owner_->Bootstrap_->GetMemoryUsageTracker());
            Cookie_.EndInsert(chunk);
            Owner_->Register(chunk);
            Cleanup();
        }

        void OnError(const TError& error)
        {
            YCHECK(!error.IsOK());
            auto wrappedError = TError("Error downloading chunk %s into cache",
                ~ToString(ChunkId_))
                << error;
            Cookie_.Cancel(wrappedError);
            LOG_WARNING(wrappedError);
            Cleanup();
        }

        void Cleanup()
        {
            Owner_.Reset();
            if (FileWriter_) {
                FileWriter_.Reset();
            }
            RemoteReader_.Reset();
            SequentialReader_.Reset();
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

TChunkCache::TChunkCache(TDataNodeConfigPtr config, TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

void TChunkCache::Initialize()
{
    Impl_->Initialize();
}

TChunkCache::~TChunkCache()
{ }

IChunkPtr TChunkCache::FindChunk(const TChunkId& chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->Find(chunkId);
}

std::vector<IChunkPtr> TChunkCache::GetChunks()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->GetChunks();
}

int TChunkCache::GetChunkCount()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->GetSize();
}

TChunkCache::TAsyncDownloadResult TChunkCache::DownloadChunk(const TChunkId& chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->Download(chunkId);
}

bool TChunkCache::IsEnabled() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->IsEnabled();
}

DELEGATE_SIGNAL(TChunkCache, void(IChunkPtr), ChunkAdded, *Impl_);
DELEGATE_SIGNAL(TChunkCache, void(IChunkPtr), ChunkRemoved, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
