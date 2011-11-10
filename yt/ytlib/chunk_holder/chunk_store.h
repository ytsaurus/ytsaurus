#pragma once

#include "common.h"

#include "../misc/cache.h"
#include "../actions/action_queue.h"
#include "../actions/signal.h"
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
    {
        YASSERT(~reader != NULL);
        BlockCount = reader->GetBlockCount();
        MasterMeta = reader->GetMasterMeta();
    }

    i32 GetBlockCount() const
    {
        return BlockCount;
    }

    const TSharedRef& GetMasterMeta() const
    {
        return MasterMeta;
    }

private:
    friend class TChunkStore;

    TSharedRef MasterMeta;
    i32 BlockCount;
};

////////////////////////////////////////////////////////////////////////////////

class TChunk;

//! Describes a physical location of chunks at a chunk holder.
class TLocation
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TLocation> TPtr;

    TLocation(Stroka path);

    //! Updates #UsedSpace and #AvailalbleSpace
    void RegisterChunk(TIntrusivePtr<TChunk> chunk);

    //! Updates #UsedSpace and #AvailalbleSpace
    void UnregisterChunk(TIntrusivePtr<TChunk> chunk);

    //! Updates #AvailalbleSpace with a system call and returns the result.
    i64 GetAvailableSpace();

    //! Returns the invoker that handles all IO requests to this location.
    IInvoker::TPtr GetInvoker() const;

    //! Returns the number of bytes used at the location.
    i64 GetUsedSpace() const;

    //! Returns the path of the location.
    Stroka GetPath() const;

    //! Returns the load factor.
    double GetLoadFactor() const;

    void IncSessionCount();
    void DecSessionCount();
    int GetSessionCount() const;

private:
    Stroka Path;
    i64 AvailableSpace;
    i64 UsedSpace;
    TActionQueue::TPtr ActionQueue;
    int SessionCount;
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
        TLocation::TPtr location)
        : Id(id)
        , Size(size)
        , Location(location)
    { }

    //! Returns chunk id.
    TChunkId GetId() const
    {
        return Id;
    }

    //! Returns the size of the chunk.
    i64 GetSize() const
    {
        return Size;
    }

    //! Returns the location of the chunk.
    TLocation::TPtr GetLocation()
    {
        return Location;
    }

private:
    friend class TChunkStore;

    TChunkId Id;
    i64 Size;
    TLocation::TPtr Location;
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
    typedef yvector<TLocation::TPtr> TLocations;

    //! Constructs a new instance.
    TChunkStore(const TChunkHolderConfig& config);

    //! Registers a just-uploaded chunk for further usage.
    TChunk::TPtr RegisterChunk(
        const TChunkId& chunkId,
        i64 size,
        TLocation::TPtr location);
    
    //! Finds chunk by id. Returns NULL if no chunk exists.
    TChunk::TPtr FindChunk(const TChunkId& chunkId);

    //! Fetches meta-information for a given chunk.
    TFuture<TChunkMeta::TPtr>::TPtr GetChunkMeta(TChunk::TPtr chunk);

    //! Returns a (cached) chunk reader.
    /*!
     *  This call is thread-safe but may block since it actually opens the file.
     *  A common rule is to invoke it only from IO thread.
     */
    TFileChunkReader::TPtr GetChunkReader(TChunk::TPtr chunk);

    //! Physically removes the chunk.
    /*!
     *  This call also evicts the reader from the cache thus hopefully closing the file.
     */
    void RemoveChunk(TChunk::TPtr chunk);

    //! Calculates a storage location for a new chunk.
    TLocation::TPtr GetNewChunkLocation();

    //! Returns a full path to a chunk file.
    Stroka GetChunkFileName(const TChunkId& chunkId, TLocation::TPtr location);

    //! Returns a full path to a chunk file.
    Stroka GetChunkFileName(TChunk::TPtr chunk);

    //! Returns the list of all registered chunks.
    TChunks GetChunks();

    //! Returns the number of registered chunks.
    int GetChunkCount();

    //! Returns locations.
    const TLocations GetLocations() const;

    //! Raised when a chunk is added.
    TParamSignal<TChunk::TPtr>& ChunkAdded();

    //! Raised when a chunk is removed.
    TParamSignal<TChunk::TPtr>& ChunkRemoved();

private:
    class TCachedReader;
    class TReaderCache;

    TChunkHolderConfig Config;
    TLocations Locations;

    typedef yhash_map<TChunkId, TChunk::TPtr> TChunkMap;
    TChunkMap ChunkMap;

    //! Caches opened chunk files.
    TIntrusivePtr<TReaderCache> ReaderCache;

    TParamSignal<TChunk::TPtr> ChunkAdded_;
    TParamSignal<TChunk::TPtr> ChunkRemoved_;

    void ScanChunks();
    void InitLocations();
    TChunkMeta::TPtr DoGetChunkMeta(TChunk::TPtr chunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

