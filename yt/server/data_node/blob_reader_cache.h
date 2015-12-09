#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/misc/error.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages cached blob chunk readers.
class TBlobReaderCache
    : public TRefCounted
{
public:
    explicit TBlobReaderCache(TDataNodeConfigPtr config);
    ~TBlobReaderCache();

    //! Returns a (cached) blob chunk reader.
    /*!
     *  This call is thread-safe but may block since it actually opens the file.
     *  A rule of thumb is to invoke it from IO thread only.
     *
     *  The returned reader is already open.
     *  
     *  This method throws on failure.
     */
    NChunkClient::TFileReaderPtr GetReader(IChunkPtr chunk);

    //! Evicts the reader from the cache thus hopefully closing the files.
    /*!
     *  NB: Do not make it TChunkPtr since it is called from TCachedBlobChunk dtor.
     */
    void EvictReader(IChunk* chunk);

private:
    class TCachedReader;
    using TCachedReaderPtr = TIntrusivePtr<TCachedReader>;

    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;

    const TImplPtr Impl_;

};

DEFINE_REFCOUNTED_TYPE(TBlobReaderCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

