#include "stdafx.h"
#include "block_store.h"
#include "private.h"
#include "chunk.h"
#include "config.h"
#include "chunk_registry.h"
#include "reader_cache.h"
#include "location.h"
#include "chunk_meta_extensions.h"
#include "bootstrap.h"

#include <ytlib/chunk_holder/chunk.pb.h>
#include <ytlib/chunk_client/file_reader.h>
#include <ytlib/chunk_client/block_cache.h>

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = DataNodeLogger;
static NProfiling::TProfiler& Profiler = DataNodeProfiler;

////////////////////////////////////////////////////////////////////////////////

TCachedBlock::TCachedBlock(
    const TBlockId& blockId,
    const TSharedRef& data,
    const TNullable<Stroka>& sourceAddress)
    : TCacheValueBase<TBlockId, TCachedBlock>(blockId)
    , Data_(data)
    , SourceAddress_(sourceAddress)
{ }

TCachedBlock::~TCachedBlock()
{
    LOG_DEBUG("Purged cached block (BlockId: %s)", ~GetKey().ToString());
}

////////////////////////////////////////////////////////////////////////////////

class TBlockStore::TStoreImpl 
    : public TWeightLimitedCache<TBlockId, TCachedBlock>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TAtomic, PendingReadSize);

    TStoreImpl(
        TChunkHolderConfigPtr config,
        TBootstrap* bootstrap)
        : TWeightLimitedCache<TBlockId, TCachedBlock>(config->MaxCachedBlocksSize)
        , Bootstrap(bootstrap)
        , PendingReadSize_(0)
    { }

    TCachedBlockPtr Put(
        const TBlockId& blockId,
        const TSharedRef& data,
        const TNullable<Stroka>& sourceAddress)
    {
        while (true) {
            TInsertCookie cookie(blockId);
            if (BeginInsert(&cookie)) {
                auto block = New<TCachedBlock>(blockId, data, sourceAddress);
                cookie.EndInsert(block);

                LOG_DEBUG("Block is put into cache (BlockId: %s, BlockSize: %" PRISZT ", SourceAddress: %s)",
                    ~blockId.ToString(),
                    data.Size(),
                    ~ToString(sourceAddress));

                return block;
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

            if (!TRef::CompareContent(data, block->GetData())) {
                LOG_FATAL("Trying to cache a block for which a different cached copy already exists (BlockId: %s)",
                    ~blockId.ToString());
            }
            
            LOG_DEBUG("Block is resurrected in cache (BlockId: %s)", ~blockId.ToString());

            return block;
        }
    }

    TAsyncGetBlockResult Get(
        const TBlockId& blockId,
        bool enableCaching)
    {
        TSharedPtr<TInsertCookie> cookie(new TInsertCookie(blockId));
        if (!BeginInsert(~cookie)) {
            LOG_DEBUG("Block cache hit (BlockId: %s)", ~blockId.ToString());
            return cookie->GetValue();
        }

        auto chunk = Bootstrap->GetChunkRegistry()->FindChunk(blockId.ChunkId);
        if (!chunk) {
            cookie->Cancel(TError(
                TChunkHolderServiceProxy::EErrorCode::NoSuchChunk,
                Sprintf("No such chunk (ChunkId: %s)", ~blockId.ChunkId.ToString())));
            return cookie->GetValue();
        }
     
        LOG_DEBUG("Block cache miss (BlockId: %s)", ~blockId.ToString());

        if (!chunk->AcquireReadLock()) {
            return MakeFuture(TGetBlockResult(TError("Cannot read chunk block %s: chunk is scheduled for removal",
                ~blockId.ToString())));
        }

        Bootstrap->GetReadPoolInvoker()->Invoke(BIND(
            &TStoreImpl::DoReadBlock,
            MakeStrong(this),
            chunk,
            blockId,
            cookie,
            enableCaching));
        
        return cookie->GetValue();
    }

    TCachedBlockPtr Find(const TBlockId& blockId)
    {
        auto asyncResult = Lookup(blockId);
        if (asyncResult.IsNull()) {
            LOG_DEBUG("Block cache miss (BlockId: %s)", ~blockId.ToString());
            return NULL;            
        }

        TNullable<TGetBlockResult> maybeResult = asyncResult.TryGet();
        if (maybeResult && maybeResult->IsOK()) {
            LOG_DEBUG("Block cache hit (BlockId: %s)", ~blockId.ToString());
            return maybeResult->Value();
        } else {
            LOG_DEBUG("Block cache miss (BlockId: %s)", ~blockId.ToString());
            return NULL;
        }
    }

private:
    TBootstrap* Bootstrap;

    virtual i64 GetWeight(TCachedBlock* block) const
    {
        return block->GetData().Size();
    }

    void DoReadBlock(
        TChunkPtr chunk,
        const TBlockId& blockId,
        TSharedPtr<TInsertCookie> cookie,
        bool enableCaching)
    {
        auto readerResult = Bootstrap->GetReaderCache()->GetReader(chunk);
        if (!readerResult.IsOK()) {
            chunk->ReleaseReadLock();
            cookie->Cancel(readerResult);
            return;
        }

        auto reader = readerResult.Value();

        const auto& chunkMeta = reader->GetChunkMeta();
        const auto blocksExt = GetProtoExtension<TBlocksExt>(chunkMeta.extensions());
        const auto& blockInfo = blocksExt.blocks(blockId.BlockIndex);
        auto blockSize = blockInfo.size();
        
        AtomicAdd(PendingReadSize_, blockSize);
        LOG_DEBUG("Pending read size increased (BlockSize: %d, PendingReadSize: %" PRISZT,
            blockSize,
            PendingReadSize_);

        auto timer = Profiler.TimingStart("/chunk_io/read_time");

        TSharedRef data;
        try {
            data = reader->ReadBlock(blockId.BlockIndex);
        } catch (const std::exception& ex) {
            LOG_FATAL("Error reading chunk block (BlockId: %s)\n%s",
                ~blockId.ToString(),
                ex.what());
        }

        chunk->ReleaseReadLock();
        
        AtomicSub(PendingReadSize_, blockSize);
        LOG_DEBUG("Pending read size decreased (BlockSize: %d, PendingReadSize: %" PRISZT,
            blockSize,
            PendingReadSize_);

        if (!data) {
            cookie->Cancel(TError(
                TChunkHolderServiceProxy::EErrorCode::NoSuchBlock,
                Sprintf("No such block (BlockId: %s)", ~blockId.ToString())));
            return;
        }

        auto block = New<TCachedBlock>(blockId, data, Null);
        cookie->EndInsert(block);

        if (!enableCaching) {
            Remove(blockId);
        }

        auto readTime = Profiler.TimingStop(timer);
        Profiler.Enqueue("/chunk_io/read_size", blockSize);
        Profiler.Enqueue("/chunk_io/read_throughput", blockSize / readTime.SecondsFloat());

        LOG_DEBUG("Finished loading block into cache (BlockId: %s)", ~blockId.ToString());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBlockStore::TCacheImpl
    : public IBlockCache
{
public:
    TCacheImpl(TIntrusivePtr<TStoreImpl> storeImpl)
        : StoreImpl(storeImpl)
    { }

    void Put(
        const TBlockId& id,
        const TSharedRef& data,
        const TNullable<Stroka>& sourceAddress)
    {
        StoreImpl->Put(id, data, sourceAddress);
    }

    TSharedRef Find(const TBlockId& id)
    {
        auto block = StoreImpl->Find(id);
        return block ? block->GetData() : TSharedRef();
    }

private:
    TIntrusivePtr<TStoreImpl> StoreImpl;

};

////////////////////////////////////////////////////////////////////////////////

TBlockStore::TBlockStore(
    TChunkHolderConfigPtr config,
    TBootstrap* bootstrap)
    : StoreImpl(New<TStoreImpl>(config, bootstrap))
    , CacheImpl(New<TCacheImpl>(StoreImpl))
{ }

TBlockStore::~TBlockStore()
{ }

TBlockStore::TAsyncGetBlockResult TBlockStore::GetBlock(
    const TBlockId& blockId,
    bool enableCaching)
{
    return StoreImpl->Get(blockId, enableCaching);
}

TCachedBlockPtr TBlockStore::FindBlock(const TBlockId& blockId)
{
    return StoreImpl->Find(blockId);
}

TCachedBlockPtr TBlockStore::PutBlock(
    const TBlockId& blockId,
    const TSharedRef& data,
    const TNullable<Stroka>& sourceAddress)
{
    return StoreImpl->Put(blockId, data, sourceAddress);
}

i64 TBlockStore::GetPendingReadSize() const
{
    return StoreImpl->GetPendingReadSize();
}

IBlockCachePtr TBlockStore::GetBlockCache()
{
    return CacheImpl;
}

std::vector<TCachedBlockPtr> TBlockStore::GetAllBlocks() const
{
    return StoreImpl->GetAll();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
