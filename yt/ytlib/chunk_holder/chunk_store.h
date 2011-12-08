#pragma once

#include "common.h"

#include "../misc/cache.h"
#include "../misc/property.h"
#include "../actions/action_queue.h"
#include "../actions/signal.h"
#include "../chunk_client/file_reader.h"

namespace NYT {
namespace NChunkHolder {

class TChunkStore;

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

    void IncrementSessionCount();
    void DecrementSessionCount();
    int GetSessionCount() const;

    //! Returns a full path to a chunk file.
    Stroka GetChunkFileName(const NChunkClient::TChunkId& chunkId) const;

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
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TChunkId, Id);
    DEFINE_BYVAL_RO_PROPERTY(TLocation::TPtr, Location);
    DEFINE_BYREF_RO_PROPERTY(NChunkHolder::NProto::TChunkInfo, Info);

public:
    typedef TIntrusivePtr<TChunk> TPtr;

    TChunk(
        const NChunkHolder::NProto::TChunkInfo& info,
        TLocation* location)
        : Id_(TGuid::FromProto(info.id()))
        , Location_(location)
        , Info_(info)
    { }

    Stroka GetFileName()
    {
        return Location_->GetChunkFileName(Id_);
    }

private:
    friend class TChunkStore;

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

    //! Registers a chunk for further usage.
    TChunk::TPtr RegisterChunk(
        const NChunkClient::TChunkId& chunkId,
        TLocation* location);
    
    //! Finds chunk by id. Returns NULL if no chunk exists.
    TChunk::TPtr FindChunk(const NChunkClient::TChunkId& chunkId) const;

    //! Returns a (cached) chunk reader.
    /*!
     *  This call is thread-safe but may block since it actually opens the file.
     *  A common rule is to invoke it only from IO thread.
     *  
     *  The returned reader is already open.
     */
    NChunkClient::TFileReader::TPtr GetChunkReader(TChunk* chunk);

    //! Physically removes the chunk.
    /*!
     *  This call also evicts the reader from the cache thus hopefully closing the file.
     */
    void RemoveChunk(TChunk* chunk);

    //! Calculates a storage location for a new chunk.
    /*!
     *  Returns a random location having the minimum number
     *  of active sessions.
     */
    TLocation::TPtr GetNewChunkLocation();

    //! Returns the list of all registered chunks.
    TChunks GetChunks() const;

    //! Returns the number of registered chunks.
    int GetChunkCount() const;

    //! Storage locations.
    DEFINE_BYREF_RO_PROPERTY(TLocations, Locations);

    //! Raised when a chunk is added.
    DEFINE_BYREF_RW_PROPERTY(TParamSignal<TChunk*>, ChunkAdded);

    //! Raised when a chunk is removed.
    DEFINE_BYREF_RW_PROPERTY(TParamSignal<TChunk*>, ChunkRemoved);

private:
    class TCachedReader;
    class TReaderCache;

    TChunkHolderConfig Config;

    typedef yhash_map<NChunkClient::TChunkId, TChunk::TPtr> TChunkMap;
    TChunkMap ChunkMap;

    //! Caches opened chunk files.
    TIntrusivePtr<TReaderCache> ReaderCache;

    void ScanChunks();
    void InitLocations();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

