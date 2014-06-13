#include "stdafx.h"
#include "block_store.h"
#include "private.h"
#include "chunk.h"
#include "config.h"
#include "chunk_registry.h"
#include "blob_reader_cache.h"
#include "location.h"

#include <ytlib/chunk_client/file_reader.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/data_node_service_proxy.h>
#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <ytlib/object_client/helpers.h>

#include <server/cell_node/bootstrap.h>

#include <core/concurrency/parallel_awaiter.h>

namespace NYT {
namespace NDataNode {

using namespace NChunkClient;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NCellNode;

using NChunkClient::NProto::TChunkMeta;
using NChunkClient::NProto::TBlocksExt;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DataNodeLogger;
static auto& Profiler = DataNodeProfiler;

static NProfiling::TRateCounter CacheReadThroughputCounter("/cache_read_throughput");

////////////////////////////////////////////////////////////////////////////////

TCachedBlock::TCachedBlock(
    const TBlockId& blockId,
    const TSharedRef& data,
    const TNullable<TNodeDescriptor>& source)
    : TCacheValueBase<TBlockId, TCachedBlock>(blockId)
    , Data_(data)
    , Source_(source)
{ }

TCachedBlock::~TCachedBlock()
{
    LOG_DEBUG("Cached block purged (BlockId: %s)", ~ToString(GetKey()));
}

////////////////////////////////////////////////////////////////////////////////

class TBlockStore::TStoreImpl
    : public TWeightLimitedCache<TBlockId, TCachedBlock>
{
public:
    TStoreImpl(
        TDataNodeConfigPtr config,
        TBootstrap* bootstrap)
        : TWeightLimitedCache<TBlockId, TCachedBlock>(config->BlockCacheSize)
        , Config_(config)
        , Bootstrap_(bootstrap)
    { }

    void Initialize()
    {
        auto result = Bootstrap_->GetMemoryUsageTracker()->TryAcquire(
            NCellNode::EMemoryConsumer::BlockCache,
            Config_->BlockCacheSize);
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error reserving memory for block cache");
    }

    void Put(
        const TBlockId& blockId,
        const TSharedRef& data,
        const TNullable<TNodeDescriptor>& source)
    {
        if (!IsCachingEnabled(blockId.ChunkId))
            return;

        while (true) {
            TInsertCookie cookie(blockId);
            if (BeginInsert(&cookie)) {
                auto block = New<TCachedBlock>(blockId, data, source);
                cookie.EndInsert(block);

                LOG_DEBUG("Block is put into cache (BlockId: %s, Size: %" PRISZT ", SourceAddress: %s)",
                    ~ToString(blockId),
                    data.Size(),
                    ~ToString(source));
                return;
            }

            auto result = cookie.GetValue().Get();
            if (!result.IsOK()) {
                // Looks like a parallel Get request has completed unsuccessfully.
                continue;
            }

            // This is a cruel reality.
            // Since we never evict blocks of removed chunks from the cache
            // it is possible for a block to be put there more than once.
            // We shall reuse the cached copy but for sanity's sake let's
            // check that the content is the same.
            auto block = result.Value();

            if (!TRef::AreBitwiseEqual(data, block->GetData())) {
                LOG_FATAL("Trying to cache block %s for which a different cached copy already exists",
                    ~ToString(blockId));
            }

            LOG_DEBUG("Block is resurrected in cache (BlockId: %s)",
                ~ToString(blockId));
            return;
        }
    }

    TFuture<TGetBlocksResult> Get(
        const TChunkId& chunkId,
        int firstBlockIndex,
        int blockCount,
        i64 priority,
        bool enableCaching);

    TCachedBlockPtr Find(const TBlockId& id)
    {
        auto block = TWeightLimitedCache::Find(id);
        if (block) {
            LogCacheHit(block);
        }
        return block;
    }

    bool IsCachingEnabled(const TChunkId& chunkId) const
    {
        // NB: No caching for journal chunks.
        auto type = TypeFromId(chunkId);
        return
            type == EObjectType::Chunk ||
            type >= EObjectType::ErasureChunkPart_0 && type <= EObjectType::ErasureChunkPart_15;
    }

    i64 GetPendingReadSize() const
    {
        return PendingReadSize_;
    }

    void UpdatePendingReadSize(i64 delta)
    {
        i64 result = (PendingReadSize_ += delta);
        LOG_DEBUG("Pending read size updated (PendingReadSize: %" PRId64 ", Delta: %" PRId64")",
            result,
            delta);
    }

private:
    friend class TGetBlocksSession;

    TDataNodeConfigPtr Config_;
    TBootstrap* Bootstrap_;

    std::atomic<i64> PendingReadSize_ = 0;


    virtual i64 GetWeight(TCachedBlock* block) const override
    {
        return block->GetData().Size();
    }

    void LogCacheHit(TCachedBlockPtr block)
    {
        Profiler.Increment(CacheReadThroughputCounter, block->GetData().Size());
        LOG_DEBUG("Block cache hit (BlockId: %s)",
            ~ToString(block->GetKey()));
    }

};

////////////////////////////////////////////////////////////////////////////////

class TBlockStore::TGetBlocksSession
    : public TIntrinsicRefCounted
{
public:
    TGetBlocksSession(
        TIntrusivePtr<TStoreImpl> storeImpl,
        const TChunkId& chunkId,
        int firstBlockIndex,
        int blockCount,
        i64 priority,
        bool enableCaching)
        : StoreImpl_(std::move(storeImpl))
        , ChunkId_(chunkId)
        , FirstBlockIndex_(firstBlockIndex)
        , BlockCount_(blockCount)
        , Priority_(priority)
        , EnableCaching_(enableCaching)
        , Promise_(NewPromise<TGetBlocksResult>())
        , Blocks_(BlockCount_)
        , Cookies_(BlockCount_)
        , BlocksPending_(BlockCount_)
    { }

    ~TGetBlocksSession()
    {
        if (LockedChunk_) {
            LockedChunk_->ReleaseReadLock();
        }
    }

    TFuture<TGetBlocksResult> Run()
    {
        int blocksToRead = BlockCount_;

        if (StoreImpl_->IsCachingEnabled(ChunkId_)) {
            // During block peering, data nodes exchange individual blocks.
            // Thus the cache may contain a block not bound to any chunk in the registry.
            // Handle these "unbound" blocks first.
            // If none is found then look for the owning chunk.
            for (int index = 0; index < BlockCount_; ++index) {
                TBlockId blockId(ChunkId_, index + FirstBlockIndex_);
                auto block = StoreImpl_->Find(blockId);
                if (block) {
                    Blocks_[index] = block->GetData();
                    OnGotBlocks(1);
                    --blocksToRead;
                } else if (EnableCaching_) {
                    auto& cookie = Cookies_[index];
                    cookie = TStoreImpl::TInsertCookie(blockId);
                    if (!StoreImpl_->BeginInsert(&cookie)) {
                        // Prevent fetching this block.
                        const static auto CachedBlockSentinel = TSharedRef::Allocate(1);
                        Blocks_[index] = CachedBlockSentinel;
                        --blocksToRead;
                        cookie.GetValue().Apply(BIND(
                            &TGetBlocksSession::OnGotBlockFromCache,
                            MakeStrong(this),
                            index));
                    }
                }
            }
        }

        auto chunkRegistry = StoreImpl_->Bootstrap_->GetChunkRegistry();
        auto chunk = chunkRegistry->FindChunk(ChunkId_);

        if (chunk) {
            if (chunk->TryAcquireReadLock()) {
                LockedChunk_ = chunk;
                chunk->ReadBlocks(FirstBlockIndex_, BlockCount_, Priority_, &Blocks_).Subscribe(BIND(
                    &TGetBlocksSession::OnGotBlocksFromChunk,
                    MakeStrong(this),
                    blocksToRead));
            } else {
                OnFail(TError(
                    "Cannot read chunk %s since it is scheduled for removal",
                    ~ToString(ChunkId_)));
            }
        }

        OnGotBlocks(0); // handle BlockCount_ == 0

        return Promise_;
    }

private:
    TIntrusivePtr<TStoreImpl> StoreImpl_;
    TChunkId ChunkId_;
    int FirstBlockIndex_;
    int BlockCount_;
    i64 Priority_;
    bool EnableCaching_;

    IChunkPtr LockedChunk_;

    std::vector<TSharedRef> Blocks_;
    std::vector<TStoreImpl::TInsertCookie> Cookies_;

    std::atomic<int> BlocksPending_;
    TPromise<TGetBlocksResult> Promise_;


    void OnGotBlocks(int count)
    {
        if ((BlocksPending_ -= count) == 0) {
            Promise_.TrySet(Blocks_);
        }
    }

    void OnFail(const TError& error)
    {
        Promise_.TrySet(error);
    }


    void OnGotBlockFromCache(int index, TErrorOr<TCachedBlockPtr> errorOrBlock)
    {
        if (errorOrBlock.IsOK()) {
            const auto& block = errorOrBlock.Value();
            StoreImpl_->LogCacheHit(block);
            Blocks_[index] = block->GetData();
            OnGotBlocks(1);
        } else {
            OnFail(errorOrBlock);
        }
    }

    void OnGotBlocksFromChunk(int count, TError error)
    {
        if (error.IsOK()) {
            OnGotBlocks(count);
            for (int index = 0; index < BlockCount_; ++index) {
                auto& cookie = Cookies_[index];
                if (cookie.IsActive()) {
                    TBlockId blockId(ChunkId_, index + FirstBlockIndex_);
                    auto cachedBlock = New<TCachedBlock>(blockId, Blocks_[index], Null);
                    cookie.EndInsert(cachedBlock);
                }
            }
        } else {
            OnFail(error);
            for (int index = 0; index < BlockCount_; ++index) {
                auto& cookie = Cookies_[index];
                if (cookie.IsActive()) {
                    cookie.Cancel(error);
                }
            }
        }
    }

};

TFuture<TBlockStore::TGetBlocksResult> TBlockStore::TStoreImpl::Get(
    const TChunkId& chunkId,
    int firstBlockIndex,
    int blockCount,
    i64 priority,
    bool enableCaching)
{
    return New<TGetBlocksSession>(
        this,
        chunkId,
        firstBlockIndex,
        std::min(blockCount, Config_->MaxRangeReadBlockCount),
        priority,
        enableCaching)->Run();
}

////////////////////////////////////////////////////////////////////////////////

class TBlockStore::TCacheImpl
    : public IBlockCache
{
public:
    explicit TCacheImpl(TIntrusivePtr<TStoreImpl> storeImpl)
        : StoreImpl_(storeImpl)
    { }

    virtual void Put(
        const TBlockId& id,
        const TSharedRef& data,
        const TNullable<TNodeDescriptor>& source) override
    {
        StoreImpl_->Put(id, data, source);
    }

    virtual TSharedRef Find(const TBlockId& id) override
    {
        auto block = StoreImpl_->Find(id);
        return block ? block->GetData() : TSharedRef();
    }

private:
    TIntrusivePtr<TStoreImpl> StoreImpl_;

};

////////////////////////////////////////////////////////////////////////////////

TBlockStore::TBlockStore(
    TDataNodeConfigPtr config,
    TBootstrap* bootstrap)
    : StoreImpl_(New<TStoreImpl>(config, bootstrap))
    , CacheImpl_(New<TCacheImpl>(StoreImpl_))
{ }

void TBlockStore::Initialize()
{
    StoreImpl_->Initialize();
}

TBlockStore::~TBlockStore()
{ }

TFuture<TBlockStore::TGetBlocksResult> TBlockStore::GetBlocks(
    const TChunkId& chunkId,
    int firstBlockIndex,
    int blockCount,
    i64 priority,
    bool enableCaching)
{
    return StoreImpl_->Get(
        chunkId,
        firstBlockIndex,
        blockCount,
        priority,
        enableCaching);
}

void TBlockStore::PutBlock(
    const TBlockId& blockId,
    const TSharedRef& data,
    const TNullable<TNodeDescriptor>& source)
{
    StoreImpl_->Put(blockId, data, source);
}

i64 TBlockStore::GetPendingReadSize() const
{
    return StoreImpl_->GetPendingReadSize();
}

void TBlockStore::UpdatePendingReadSize(i64 delta)
{
    StoreImpl_->UpdatePendingReadSize(delta);
}

IBlockCachePtr TBlockStore::GetBlockCache()
{
    return CacheImpl_;
}

std::vector<TCachedBlockPtr> TBlockStore::GetAllBlocks() const
{
    return StoreImpl_->GetAll();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
