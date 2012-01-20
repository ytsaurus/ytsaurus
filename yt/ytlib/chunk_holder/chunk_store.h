#pragma once

#include "common.h"
#include "chunk.h"
#include "location.h"
#include "reader_cache.h"

#include <ytlib/misc/cache.h>
#include <ytlib/misc/property.h>
#include <ytlib/actions/action_queue.h>
#include <ytlib/actions/signal.h>
#include <ytlib/chunk_client/file_reader.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Manages stored chunks.
class TChunkStore
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkStore> TPtr;
    typedef yvector<TStoredChunk::TPtr> TChunks;
    typedef yvector<TLocation::TPtr> TLocations;

    //! Constructs a new instance.
    TChunkStore(
        TChunkHolderConfig* config,
        TReaderCache* readerCache);

    //! Registers a chunk.
    void RegisterChunk(TStoredChunk* chunk);
    
    //! Finds chunk by id. Returns NULL if no chunk exists.
    TStoredChunk::TPtr FindChunk(const TChunkId& chunkId) const;

    //! Physically removes the chunk.
    /*!
     *  This call also evicts the reader from the cache thus hopefully closing the file.
     */
    void RemoveChunk(TStoredChunk* chunk);

    //! Calculates a storage location for a new chunk.
    /*!
     *  Among not full locations returns a random location having the minimum number
     *  of active sessions. Throws exception of all locations are full
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
    TChunkHolderConfig::TPtr Config;
    TReaderCache::TPtr ReaderCache;

    typedef yhash_map<TChunkId, TStoredChunk::TPtr> TChunkMap;
    TChunkMap ChunkMap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

