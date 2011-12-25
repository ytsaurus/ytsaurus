#include "stdafx.h"
#include "block_store.h"
#include "chunk_store.h"
#include "chunk_cache.h"
#include "reader_cache.h"

#include "../chunk_client/file_reader.h"

#include "../misc/assert.h"

namespace NYT {
namespace NChunkHolder {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TCachedBlock::TCachedBlock(const TBlockId& blockId, const TSharedRef& data)
    : TCacheValueBase<TBlockId, TCachedBlock>(blockId)
    , Data(data)
{ }

TSharedRef TCachedBlock::GetData() const
{
    return Data;
}

TCachedBlock::~TCachedBlock()
{
    LOG_DEBUG("Purged cached block (BlockId: %s)", ~GetKey().ToString());
}

////////////////////////////////////////////////////////////////////////////////

class TBlockStore::TStoreImpl 
    : public TWeightLimitedCache<TBlockId, TCachedBlock>
{
public:
    typedef TIntrusivePtr<TStoreImpl> TPtr;

    DEFINE_BYVAL_RO_PROPERTY(TAtomic, PendingReadSize);

    TStoreImpl(
        TChunkHolderConfig* config,
        TChunkStore* chunkStore,
        TChunkCache* chunkCache,
        TReaderCache* readerCache)
        : TWeightLimitedCache<TBlockId, TCachedBlock>(config->MaxCachedBlocksSize)
        , ChunkStore(chunkStore)
        , ChunkCache(chunkCache)
        , ReaderCache(readerCache)
        , PendingReadSize_(0)
    { }

    TCachedBlock::TPtr Put(const TBlockId& blockId, const TSharedRef& data)
    {
        TInsertCookie cookie(blockId);
        if (BeginInsert(&cookie)) {
            auto block = New<TCachedBlock>(blockId, data);
            cookie.EndInsert(block);

            LOG_DEBUG("Block is put into cache (BlockId: %s, BlockSize: %" PRISZT ")",
                ~blockId.ToString(),
                data.Size());

            return block;
        } else {
            // This is a cruel reality.
            // Since we never evict blocks of removed chunks from the cache
            // it is possible for a block to be put there more than once.
            // We shall reuse the cached copy but for sanity's sake let's
            // check that the content is the same.
            auto result = cookie.GetAsyncResult()->Get();
            YASSERT(result.IsOK());
            auto block = result.Value();

            if (!TRef::CompareContent(data, block->GetData())) {
                LOG_FATAL("Trying to cache a block for which a different cached copy already exists (BlockId: %s)",
                    ~blockId.ToString());
            }

            LOG_DEBUG("Block is resurrected in cache (BlockId: %s)", ~blockId.ToString());

            return block;
        }
    }

    TAsyncGetBlockResult::TPtr Get(const TBlockId& blockId)
    {
        TSharedPtr<TInsertCookie> cookie(new TInsertCookie(blockId));
        if (!BeginInsert(~cookie)) {
            LOG_DEBUG("Block cache hit (BlockId: %s)", ~blockId.ToString());
            return cookie->GetAsyncResult();
        }

        auto chunk = FindChunk(blockId.ChunkId);
        if (!chunk) {
            return ToFuture(TGetBlockResult(
                TChunkHolderServiceProxy::EErrorCode::NoSuchChunk,
                Sprintf("No such chunk (ChunkId: %s)", ~blockId.ChunkId.ToString())));
        }
     
        LOG_DEBUG("Block cache miss (BlockId: %s)", ~blockId.ToString());

        auto invoker = chunk->GetLocation()->GetInvoker();
        invoker->Invoke(FromMethod(
            &TStoreImpl::DoReadBlock,
            TPtr(this),
            chunk,
            blockId,
            cookie));
        
        return cookie->GetAsyncResult();
    }

    TCachedBlock::TPtr Find(const TBlockId& blockId)
    {
        auto asyncResult = Lookup(blockId);
        TGetBlockResult result;
        if (asyncResult && asyncResult->TryGet(&result) && result.IsOK()) {
            LOG_DEBUG("Block cache hit (BlockId: %s)", ~blockId.ToString());
            return result.Value();
        } else {
            LOG_DEBUG("Block cache miss (BlockId: %s)", ~blockId.ToString());
            return NULL;
        }
    }

private:
    TChunkStore::TPtr ChunkStore;
    TChunkCache::TPtr ChunkCache;
    TReaderCache::TPtr ReaderCache;

    TChunk::TPtr FindChunk(const TChunkId& chunkId)
    {
        // There are two possible places where we can look for a chunk: ChunkStore and ChunkCache.
        auto storedChunk = ChunkStore->FindChunk(chunkId);
        if (storedChunk) {
            return storedChunk;
        }

        auto cachedChunk = ChunkCache->FindChunk(chunkId);
        if (cachedChunk) {
            return cachedChunk;
        }

        return NULL;
    }

    virtual i64 GetWeight(TCachedBlock* block) const
    {
        return block->GetData().Size();
    }

    void DoReadBlock(
        TChunk::TPtr chunk,
        const TBlockId& blockId,
        TSharedPtr<TInsertCookie> cookie)
    {
        auto readerResult = ReaderCache->GetReader(~chunk);
        if (!readerResult.IsOK()) {
            cookie->Cancel(readerResult);
            return;
        }

        auto reader = readerResult.Value();

        const auto& chunkInfo = reader->GetChunkInfo();
        const auto& blockInfo = chunkInfo.blocks(blockId.BlockIndex);
        auto blockSize = blockInfo.size();
        
        AtomicAdd(PendingReadSize_, blockSize);
        LOG_DEBUG("Pending read size increased (BlockSize: %d, PendingReadSize: %" PRISZT,
            blockSize,
            PendingReadSize_);

        auto data = reader->ReadBlock(blockId.BlockIndex);
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

        auto block = New<TCachedBlock>(blockId, data);
        cookie->EndInsert(block);

        LOG_DEBUG("Finished loading block into cache (BlockId: %s)", ~blockId.ToString());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBlockStore::TCacheImpl
    : public IBlockCache
{
public:
    TCacheImpl(TStoreImpl* storeImpl)
        : StoreImpl(storeImpl)
    { }

    void Put(const TBlockId& id, const TSharedRef& data)
    {
        StoreImpl->Put(id, data);
    }

    TSharedRef Find(const TBlockId& id)
    {
        auto block = StoreImpl->Find(id);
        return block ? block->GetData() : TSharedRef();
    }

private:
    TStoreImpl::TPtr StoreImpl;

};

////////////////////////////////////////////////////////////////////////////////

TBlockStore::TBlockStore(
    TChunkHolderConfig* config,
    TChunkStore* chunkStore,
    TChunkCache* chunkCache,
    TReaderCache* readerCache)
    : StoreImpl(New<TStoreImpl>(
        config,
        chunkStore,
        chunkCache,
        readerCache))
    , CacheImpl(New<TCacheImpl>(~StoreImpl))
{ }

TBlockStore::TAsyncGetBlockResult::TPtr TBlockStore::GetBlock(const TBlockId& blockId)
{
    return StoreImpl->Get(blockId);
}

TCachedBlock::TPtr TBlockStore::PutBlock(const TBlockId& blockId, const TSharedRef& data)
{
    return StoreImpl->Put(blockId, data);
}

i64 TBlockStore::GetPendingReadSize() const
{
    return StoreImpl->GetPendingReadSize();
}

IBlockCache* TBlockStore::GetCache()
{
    return ~CacheImpl;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
