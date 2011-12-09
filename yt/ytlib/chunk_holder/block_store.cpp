#include "stdafx.h"
#include "block_store.h"
#include "chunk_store.h"
#include "reader_cache.h"

#include "../chunk_client/file_reader.h"

#include "../misc/assert.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

class TBlockStore::TBlockCache 
    : public TCapacityLimitedCache<TBlockId, TCachedBlock>
{
public:
    typedef TIntrusivePtr<TBlockCache> TPtr;

    TBlockCache(
        const TChunkHolderConfig& config,
        TChunkStore* chunkStore,
        TReaderCache* readerCache)
        : TCapacityLimitedCache<TBlockId, TCachedBlock>(config.MaxCachedBlocks)
        , ChunkStore(chunkStore)
        , ReaderCache(readerCache)
    { }

    TCachedBlock::TPtr Put(const TBlockId& blockId, const TSharedRef& data)
    {
        TInsertCookie cookie(blockId);
        if (BeginInsert(&cookie)) {
            auto block = New<TCachedBlock>(blockId, data);
            cookie.EndInsert(block);

            LOG_DEBUG("Block is put into cache (BlockId: %s)",
                ~blockId.ToString());
            return block;
        } else {
            // This is a cruel reality.
            // Since we never evict blocks of removed chunks from the cache
            // it is possible for a block to be put there more than once.
            // We shall reuse the cached copy but for sanity's sake let's
            // check that the content is the same.
            auto block = cookie.GetAsyncResult()->Get();
            if (!TRef::CompareContent(data, block->GetData())) {
                LOG_FATAL("Trying to cache a block for which a different cached copy already exists (BlockId: %s)",
                    ~blockId.ToString());
            }
            LOG_DEBUG("Block is resurrected in cache (BlockId: %s)",
                ~blockId.ToString());
            return block;
        }
    }

    TCachedBlock::TAsync::TPtr Find(const TBlockId& blockId)
    {
        TAutoPtr<TInsertCookie> cookie(new TInsertCookie(blockId));
        if (!BeginInsert(~cookie)) {
            LOG_DEBUG("Block is found in cache (BlockId: %s)",
                ~blockId.ToString());
            return cookie->GetAsyncResult();
        }

        auto chunk = ChunkStore->FindChunk(blockId.ChunkId);
        if (~chunk == NULL) {
            return New<TCachedBlock::TAsync>(TCachedBlock::TPtr(NULL));
        }
        
        LOG_DEBUG("Loading block into cache (BlockId: %s)",
            ~blockId.ToString());

        auto result = cookie->GetAsyncResult();

        auto invoker = chunk->GetLocation()->GetInvoker();
        invoker->Invoke(FromMethod(
            &TBlockCache::DoReadBlock,
            TPtr(this),
            chunk,
            blockId,
            cookie));

        return result;
    }

private:
    TChunkStore::TPtr ChunkStore;
    TReaderCache::TPtr ReaderCache;

    void DoReadBlock(
        TChunk::TPtr chunk,
        const TBlockId& blockId,
        TAutoPtr<TInsertCookie> cookie)
    {
        try {
            auto reader = ReaderCache->FindReader(~chunk);
            if (~reader == NULL) {
                LOG_WARNING("Attempt to read a block from a non-existing chunk (BlockId: %s)", ~blockId.ToString());
                return;
            }

            auto data = reader->ReadBlock(blockId.BlockIndex);
            if (~data == NULL) {
                LOG_WARNING("Attempt to read a non-existing block (BlockId: %s)", ~blockId.ToString());
            }

            auto block = New<TCachedBlock>(blockId, data);
            cookie->EndInsert(block);

            LOG_DEBUG("Finished loading block into cache (BlockId: %s)", ~blockId.ToString());
        } catch (...) {
            LOG_FATAL("Error loading block into cache (BlockId: %s)\n%s",
                ~blockId.ToString(),
                ~CurrentExceptionMessage());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TBlockStore::TBlockStore(
    const TChunkHolderConfig& config,
    TChunkStore* chunkStore,
    TReaderCache* readerCache)
    : BlockCache(New<TBlockCache>(config, chunkStore, readerCache))
{ }

TCachedBlock::TAsync::TPtr TBlockStore::FindBlock(const TBlockId& blockId)
{
    LOG_DEBUG("Getting block from store (BlockId: %s)",
        ~blockId.ToString());

    return BlockCache->Find(blockId);
}

TCachedBlock::TPtr TBlockStore::PutBlock(const TBlockId& blockId, const TSharedRef& data)
{
    LOG_DEBUG("Putting block into store (BlockId: %s, BlockSize: %d)",
        ~blockId.ToString(),
        static_cast<int>(data.Size()));

    return BlockCache->Put(blockId, data);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
