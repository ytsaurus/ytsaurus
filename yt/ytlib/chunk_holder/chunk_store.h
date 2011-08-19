#pragma once

#include "common.h"

#include "../misc/cache.h"
#include "../actions/action_queue.h"
#include "../chunk_client/file_chunk_reader.h"

namespace NYT {
namespace NChunkHolder {

class TChunkStore;

////////////////////////////////////////////////////////////////////////////////

//! Describes chunk meta-information.
/*!
 *  This class holds some useful pieces of information that
 *  is impossible to fetch during holder startup since it requires reading chunk files.
 */
class TChunkMeta
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkMeta> TPtr;

    TChunkMeta(TFileChunkReader::TPtr reader)
        : BlockCount(reader->GetBlockCount())
    { }

    i32 GetBlockCount() const
    {
        return BlockCount;
    }

private:
    friend class TChunkStore;

    i32 BlockCount;

};

////////////////////////////////////////////////////////////////////////////////

//! Describes chunk at a chunk holder.
class TChunk
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunk> TPtr;

    TChunk(
        const TChunkId& id,
        i64 size,
        int location)
        : Id(id)
        , Size(size)
        , Location(location)
    { }

    //! Returns chunk id.
    TChunkId GetId() const
    {
        return Id;
    }

    //! Returns chunk size.
    i64 GetSize() const
    {
        return Size;
    }

    //! Returns chunk storage location.
    int GetLocation() const
    {
        return Location;
    }

private:
    friend class TChunkStore;

    TChunkId Id;
    i64 Size;
    int Location;
    TChunkMeta::TPtr Meta;

};

////////////////////////////////////////////////////////////////////////////////

//! Manages uploaded chunks.
class TChunkStore
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkStore> TPtr;
    typedef yvector<TChunk::TPtr> TChunks;

    //! Constructs a new instance.
    TChunkStore(const TChunkHolderConfig& config);

    //! Registers a just-uploaded chunk for further usage.
    TChunk::TPtr RegisterChunk(
        const TChunkId& chunkId,
        i64 size,
        int location);
    
    //! Finds chunk by id. Returns NULL if no chunk exists.
    TChunk::TPtr FindChunk(const TChunkId& chunkId);

    //! Fetches meta-information for a given chunk.
    TAsyncResult<TChunkMeta::TPtr>::TPtr GetChunkMeta(TChunk::TPtr chunk);

    //! Returns a (cached) chunk reader.
    /*!
     *  This call is thread-safe but may block since it actually opens the file.
     *  A common rule is to invoke it only from IO thread.
     */
    TFileChunkReader::TPtr GetChunkReader(TChunk::TPtr chunk);

    //! Returns invoker for a given storage location.
    IInvoker::TPtr GetIOInvoker(int location);

    //! Calculates a storage location for a new chunk.
    int GetNewChunkLocation(const TChunkId& chunkId);

    //! Returns a full path to a chunk file.
    Stroka GetChunkFileName(const TChunkId& chunkId, int location);

    //! Returns current statistics.
    THolderStatistics GetStatistics() const;

    //! Returns the list of all registered chunks.
    TChunks GetChunks();

private:
    class TCachedReader;
    class TReaderCache;

    TChunkHolderConfig Config; // TODO: avoid copying

    //! Actions queues that handle IO requests to chunk storage locations.
    yvector<IInvoker::TPtr> IOInvokers;

    typedef yhash_map<TChunkId, TChunk::TPtr, TGuidHash> TChunkMap;
    TChunkMap ChunkMap;

    //! Caches opened chunk files.
    TIntrusivePtr<TReaderCache> ReaderCache;

    void ScanChunks();
    void InitIOQueues();
    TChunkMeta::TPtr DoGetChunkMeta(TChunk::TPtr chunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

