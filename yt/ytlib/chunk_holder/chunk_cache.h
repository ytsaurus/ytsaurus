#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../misc/error.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Manages cached chunks.
class TChunkCache
{
public:
    typedef TIntrusivePtr<TChunkCache> TPtr;
    typedef yvector<TChunk::TPtr> TChunks;

    //! Finds chunk by id. Returns NULL if no chunk exists.
    TChunk::TPtr FindChunk(const NChunkClient::TChunkId& chunkId) const;

    //! Returns the list of all registered chunks.
    TChunks GetChunks() const;

    //! Returns the number of registered chunks.
    int GetChunkCount() const;

    typedef TValuedError<TChunk::TPtr> TDownloadResult;
    typedef TFuture<TDownloadResult> TAsyncDownloadResult;

    //! Downloads a chunk into the cache.
    /*!
     *  The download process is asynchronous.
     *  If the chunk is already cached, it returns a set result.
     */
    TAsyncDownloadResult::TPtr DownloadChunk(const TChunkId& chunkId);

    //! Raised when a chunk is added.
    DEFINE_BYREF_RW_PROPERTY(TParamSignal<TChunk*>, ChunkAdded);

    //! Raised when a chunk is removed.
    DEFINE_BYREF_RW_PROPERTY(TParamSignal<TChunk*>, ChunkRemoved);

private:

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

