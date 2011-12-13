#include "stdafx.h"
#include "block_store.h"
#include "chunk_store.h"
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

class TBlockStore::TBlockCache 
    : public TWeightLimitedCache<TBlockId, TCachedBlock>
{
public:
    typedef TIntrusivePtr<TBlockCache> TPtr;

    TBlockCache(
        TChunkHolderConfig* config,
        TChunkStore* chunkStore,
        TReaderCache* readerCache)
        : TWeightLimitedCache<TBlockId, TCachedBlock>(config->MaxCachedBlocksSize)
        , ChunkStore(chunkStore)
        , ReaderCache(readerCache)
    { }

    TCachedBlock::TPtr Put(const TBlockId& blockId, const TSharedRef& data)
    {
        LOG_DEBUG("Putting block into store (BlockId: %s, BlockSize: %d)",
            ~blockId.ToString(),
            static_cast<int>(data.Size()));

        TInsertCookie cookie(blockId);
        if (BeginInsert(&cookie)) {
            auto block = New<TCachedBlock>(blockId, data);
            cookie.EndInsert(block);
            LOG_DEBUG("Block is put into cache (BlockId: %s)", ~blockId.ToString());
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
        LOG_DEBUG("Getting block from store (BlockId: %s)", ~blockId.ToString());

        TSharedPtr<TInsertCookie> cookie(new TInsertCookie(blockId));
        if (!BeginInsert(~cookie)) {
            LOG_DEBUG("Block is already cached (BlockId: %s)", ~blockId.ToString());
            return cookie->GetAsyncResult();
        }

        auto chunk = ChunkStore->FindChunk(blockId.ChunkId);
        if (~chunk == NULL) {
            return ToFuture(TGetBlockResult(
                TChunkHolderServiceProxy::EErrorCode::NoSuchChunk,
                Sprintf("No such chunk (ChunkId: %s)", ~blockId.ChunkId.ToString())));
        }
        
        LOG_DEBUG("Loading block into cache (BlockId: %s)", ~blockId.ToString());

        auto invoker = chunk->GetLocation()->GetInvoker();
        invoker->Invoke(FromMethod(
            &TBlockCache::DoReadBlock,
            TPtr(this),
            chunk,
            blockId,
            cookie));

        return cookie->GetAsyncResult();
    }

private:
    TChunkStore::TPtr ChunkStore;
    TReaderCache::TPtr ReaderCache;

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
        auto data = reader->ReadBlock(blockId.BlockIndex);
        if (~data == NULL) {
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

TBlockStore::TBlockStore(
    TChunkHolderConfig* config,
    TChunkStore* chunkStore,
    TReaderCache* readerCache)
    : BlockCache(New<TBlockCache>(config, chunkStore, readerCache))
{ }

TBlockStore::TAsyncGetBlockResult::TPtr TBlockStore::GetBlock(const TBlockId& blockId)
{
    return BlockCache->Get(blockId);
}

TCachedBlock::TPtr TBlockStore::PutBlock(const TBlockId& blockId, const TSharedRef& data)
{
    return BlockCache->Put(blockId, data);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
