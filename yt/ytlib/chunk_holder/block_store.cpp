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

class TBlockStore::TCachedFile
    : public TCacheValueBase<TChunkId, TCachedFile, TGUIDHash>
{
public:
    typedef TIntrusivePtr<TCachedFile> TPtr;

    TCachedFile(const TChunkId& chunkId, Stroka fileName)
        : TCacheValueBase<TChunkId, TCachedFile, TGUIDHash>(chunkId)
        , File_(fileName, OpenExisting|RdOnly)
    { }


    TFile& File()
    {
        return File_;
    }

private:
    TFile File_;

};


////////////////////////////////////////////////////////////////////////////////

class TBlockStore::TFileCache
    : public TCapacityLimitedCache<TChunkId, TCachedFile, TGUIDHash>
{
public:
    typedef TIntrusivePtr<TFileCache> TPtr;

    TFileCache(
        const TChunkHolderConfig& config,
        TChunkStore::TPtr chunkStore)
        : TCapacityLimitedCache<TChunkId, TCachedFile, TGUIDHash>(config.MaxCachedFiles)
        , ChunkStore(chunkStore)
    { }

    TCachedFile::TPtr Get(TChunk::TPtr chunk)
    {
        TInsertCookie cookie(chunk->GetId());
        if (BeginInsert(&cookie)) {
            // TODO: IO exceptions and error checking
            TCachedFile::TPtr file = new TCachedFile(
                chunk->GetId(),
                ChunkStore->GetChunkFileName(chunk->GetId(), chunk->GetLocation()));
            EndInsert(file, &cookie);
        }
        return cookie.GetAsyncResult()->Get();
    }

private:
    TChunkStore::TPtr ChunkStore;

};

////////////////////////////////////////////////////////////////////////////////

class TBlockStore::TBlockCache 
    : public TCapacityLimitedCache<TBlockId, TCachedBlock, TBlockIdHash>
{
public:
    typedef TIntrusivePtr<TBlockCache> TPtr;

    TBlockCache(
        const TChunkHolderConfig& config,
        TChunkStore::TPtr chunkStore,
        TFileCache::TPtr fileCache)
        : TCapacityLimitedCache<TBlockId, TCachedBlock, TBlockIdHash>(config.MaxCachedBlocks)
        , ChunkStore(chunkStore)
        , FileCache(fileCache)
    { }

    TCachedBlock::TPtr Put(const TBlockId& blockId, const TSharedRef& data)
    {
        TInsertCookie cookie(blockId);
        // TODO: use YVERIFY
        VERIFY(BeginInsert(&cookie), "oops");
        TCachedBlock::TPtr block = new TCachedBlock(blockId, data);
        EndInsert(block, &cookie);
        return block;
    }

    TCachedBlock::TAsync::TPtr Find(const TBlockId& blockId, i32 blockSize)
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
    TFileCache::TPtr FileCache;

    void ReadBlock(
        TChunk::TPtr chunk,
        const TBlockId& blockId,
        i32 blockSize,
        TAutoPtr<TInsertCookie> cookie)
    {
        // TODO: IO exceptions and error checking

        TFile& file = FileCache->Get(chunk)->File();
        TBlob data(blockSize);
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
    : FileCache(new TFileCache(config, chunkStore))
    , BlockCache(new TBlockCache(config, chunkStore, FileCache))
{ }

TCachedBlock::TAsync::TPtr TBlockStore::FindBlock(const TBlockId& blockId, i32 blockSize)
{
    LOG_DEBUG("Getting block from store (BlockId: %s, BlockSize: %d)",
        ~blockId.ToString(),
        blockSize);

    return BlockCache->Find(blockId, blockSize);
}

TCachedBlock::TPtr TBlockStore::PutBlock(const TBlockId& blockId, const TSharedRef& data)
{
    LOG_DEBUG("Putting block into store (BlockId: %s, BlockSize: %d)",
        ~blockId.ToString(),
        data.Size());

    return BlockCache->Put(blockId, data);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
