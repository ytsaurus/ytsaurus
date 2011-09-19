#include "block_store.h"
#include "chunk_store.h"

#include "../chunk_client/file_chunk_reader.h"

#include "../misc/assert.h"

namespace NYT {
namespace NChunkHolder {

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
        TChunkStore::TPtr chunkStore)
        : TCapacityLimitedCache<TBlockId, TCachedBlock>(config.MaxCachedBlocks)
        , ChunkStore(chunkStore)
    { }

    TCachedBlock::TPtr Put(const TBlockId& blockId, const TSharedRef& data)
    {
        while (true) {
            TInsertCookie cookie(blockId);
            if (!BeginInsert(&cookie)) {
                // This is a cruel reality.
                // Since we never evict blocks of removed chunks from the cache
                // it is possible for a block to be put there more than once.
                // We could reuse the cached copy but for sanity's sake let's
                // replace the cached one.
                TCacheBase::Remove(blockId);
                continue;
            }
            TCachedBlock::TPtr block = New<TCachedBlock>(blockId, data);
            EndInsert(block, &cookie);
            return block;
        }
    }

    TCachedBlock::TAsync::TPtr Find(const TBlockId& blockId)
    {
        TAutoPtr<TInsertCookie> cookie(new TInsertCookie(blockId));
        if (!BeginInsert(~cookie)) {
            LOG_DEBUG("Got cached block from store (BlockId: %s)",
                ~blockId.ToString());
            return cookie->GetAsyncResult();
        }

        TChunk::TPtr chunk = ChunkStore->FindChunk(blockId.ChunkId);
        if (~chunk == NULL)
            return NULL;
        
        LOG_DEBUG("Loading block into cache (BlockId: %s)",
            ~blockId.ToString());

        TCachedBlock::TAsync::TPtr result = cookie->GetAsyncResult();

        IInvoker::TPtr invoker = chunk->GetLocation()->GetInvoker();
        invoker->Invoke(FromMethod(
            &TBlockCache::ReadBlock,
            TPtr(this),
            chunk,
            blockId,
            cookie));

        return result;
    }

private:
    TChunkStore::TPtr ChunkStore;

    void ReadBlock(
        TChunk::TPtr chunk,
        const TBlockId& blockId,
        TAutoPtr<TInsertCookie> cookie)
    {
        try {
            TFileChunkReader::TPtr reader = ChunkStore->GetChunkReader(chunk);
            TSharedRef data = reader->ReadBlock(blockId.BlockIndex);
            if (data != TSharedRef()) {
                TCachedBlock::TPtr cachedBlock = New<TCachedBlock>(blockId, data);
                EndInsert(cachedBlock, ~cookie);

                LOG_DEBUG("Finished loading block into cache (BlockId: %s)", ~blockId.ToString());
            } else {
                LOG_WARNING("Attempt to read a non-existing block (BlockId: %s)", ~blockId.ToString());
            }
        } catch (...) {
            LOG_FATAL("Error loading block into cache (BlockId: %s, What: %s)",
                ~blockId.ToString(),
                ~CurrentExceptionMessage());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TBlockStore::TBlockStore(
    const TChunkHolderConfig& config,
    TChunkStore::TPtr chunkStore)
    : BlockCache(New<TBlockCache>(config, chunkStore))
{ }

TCachedBlock::TAsync::TPtr TBlockStore::FindBlock(const TBlockId& blockId)
{
    LOG_DEBUG("Getting block from store (BlockId: %s)", ~blockId.ToString());

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
