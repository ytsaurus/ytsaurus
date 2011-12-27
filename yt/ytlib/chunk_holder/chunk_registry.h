#pragma once

#include "common.h"
#include "chunk.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////
   
class TChunkStore;
class TChunkCache;

//! A facade for locating chunks that are fully uploaded to the chunk holder.
/*!
 *  Uploaded chunks can be registered either at TChunkStore or at TChunkCache.
 *  This class provides a single entry point for locating these chunks.
 */
class TChunkRegistry
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkRegistry> TPtr;

    //! Finds chunk by id. Returns NULL if no chunk exists.
    TChunk::TPtr FindChunk(const NChunkClient::TChunkId& chunkId) const;

    // Due to cyclic dependency these values cannot be injected via ctor.
    void SetChunkStore(TChunkStore* chunkStore);
    void SetChunkCache(TChunkCache* chunkCache);

private:
    TChunkStore* ChunkStore;
    TChunkCache* ChunkCache;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

