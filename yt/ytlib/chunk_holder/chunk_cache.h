#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/error.h>
#include <ytlib/actions/signal.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Manages cached chunks.
/*!
 *  \note
 *  Thread affinity: any.
 *  
 *  Since methods may be called from arbitrary threads some of their results
 *  may only be regarded as a transient snapshot
 *  (applies to #GetChunks, #FindChunk, #GetChunkCount);
 */
class TChunkCache
    : public TRefCounted
{
public:
    typedef std::vector<TCachedChunkPtr> TChunks;

    //! Constructs a new instance.
    TChunkCache(TDataNodeConfigPtr config, TBootstrap* bootstrap);

    //! Initializes the cache.
    void Start();

    ~TChunkCache();

    //! Finds chunk by id. Returns NULL if no chunk exists.
    TCachedChunkPtr FindChunk(const TChunkId& chunkId);

    //! Returns the list of all registered chunks.
    TChunks GetChunks();

    //! Returns the number of registered chunks.
    int GetChunkCount();

    const TGuid& GetCellGuid() const;
    void UpdateCellGuid(const TGuid& cellGuid);

    typedef TValueOrError<TCachedChunkPtr> TDownloadResult;
    typedef TFuture<TDownloadResult> TAsyncDownloadResult;

    //! Downloads a chunk into the cache.
    /*!
     *  The download process is asynchronous.
     *  If the chunk is already cached, it returns a pre-set result.
     */
    TAsyncDownloadResult DownloadChunk(
        const TChunkId& chunkId,
        const std::vector<Stroka>& seedAddresses = std::vector<Stroka>());

    //! Raised when a chunk is added to the cache.
    DECLARE_SIGNAL(void(TChunkPtr), ChunkAdded);

    //! Raised when a chunk is removed from the cache.
    DECLARE_SIGNAL(void(TChunkPtr), ChunkRemoved);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

