#include "block_store.h"
#include "chunk_store.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TCachedBlock::TCachedBlock(const TBlockId& blockId, const TSharedRef& data)
    : TCacheValueBase<TBlockId, TCachedBlock, TBlockIdHash>(blockId)
    , Data(data)
{ }

TSharedRef TCachedBlock::GetData() const
{
    return Data;
}

////////////////////////////////////////////////////////////////////////////////

class TBlockStore::TBlockCache 
    : public TCapacityLimitedCache<TBlockId, TCachedBlock, TBlockIdHash>
{
public:
    typedef TIntrusivePtr<TBlockCache> TPtr;

    TBlockCache(
        const TChunkHolderConfig& config,
        TChunkStore::TPtr chunkStore)
        : TCapacityLimitedCache<TBlockId, TCachedBlock, TBlockIdHash>(config.CacheCapacity)
        , ChunkStore(chunkStore)
    { }

    TCachedBlock::TPtr Put(const TBlockId& blockId, const TSharedRef& data)
    {
        TInsertCookie cookie(blockId);
        VERIFY(BeginInsert(&cookie), "oops");
        TCachedBlock::TPtr block = new TCachedBlock(blockId, data);
        EndInsert(block, &cookie);
        return block;
    }

    TCachedBlock::TAsync::TPtr Get(const TBlockId& blockId, i32 blockSize)
    {
        TAutoPtr<TInsertCookie> cookie = new TInsertCookie(blockId);
        if (!BeginInsert(~cookie)) {
            LOG_DEBUG("Got cached block from store (BlockId: %s, BlockSize: %d)",
                ~blockId.ToString(),
                blockSize);
            return cookie->GetAsyncResult();
        }

        TChunk::TPtr chunk = ChunkStore->FindChunk(blockId.ChunkId);
        if (~chunk == NULL)
            return NULL;
        
        LOG_DEBUG("Loading block into cache (BlockId: %s, BlockSize: %d)",
            ~blockId.ToString(),
            blockSize);

        TCachedBlock::TAsync::TPtr result = cookie->GetAsyncResult();
        int location = chunk->GetLocation();
        IInvoker::TPtr invoker = ChunkStore->GetIOInvoker(location);
        invoker->Invoke(FromMethod(
            &TBlockCache::ReadBlock,
            TPtr(this),
            chunk,
            blockId,
            blockSize,
            cookie));

        return result;
    }

private:
    TChunkStore::TPtr ChunkStore;

    void ReadBlock(
        TChunk::TPtr chunk,
        const TBlockId& blockId,
        i32 blockSize,
        TAutoPtr<TInsertCookie> cookie)
    {
        // TODO: IO exceptions and error checking

        Stroka fileName = ChunkStore->GetChunkFileName(chunk->GetId(), chunk->GetLocation());
       
        TBlob data(blockSize);
        TFile file(fileName, OpenExisting|RdOnly|Seq|Direct);
        file.Pread(data.begin(), blockSize, blockId.Offset);
        
        TCachedBlock::TPtr block = new TCachedBlock(blockId, data);
        
        EndInsert(block, ~cookie);

        LOG_DEBUG("Finished loading block into cache (BlockId: %s, BlockSize: %d)",
            ~blockId.ToString(),
            blockSize);
    }
};

////////////////////////////////////////////////////////////////////////////////

TBlockStore::TBlockStore(
    const TChunkHolderConfig& config,
    TChunkStore::TPtr chunkStore)
    : Cache(new TBlockCache(config, chunkStore))
{ }

TCachedBlock::TAsync::TPtr TBlockStore::GetBlock(const TBlockId& blockId, i32 blockSize)
{
    LOG_DEBUG("Getting block from store (BlockId: %s, BlockSize: %d)",
        ~blockId.ToString(),
        blockSize);

    return Cache->Get(blockId, blockSize);
}

TCachedBlock::TPtr TBlockStore::PutBlock(const TBlockId& blockId, const TSharedRef& data)
{
    LOG_DEBUG("Putting block into store (BlockId: %s, BlockSize: %d)",
        ~blockId.ToString(),
        data.Size());

    return Cache->Put(blockId, data);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
