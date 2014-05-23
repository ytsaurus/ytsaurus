#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages cached blob chunk readers.
class TBlobReaderCache
    : public TRefCounted
{
public:
    //! Constructs a new instance.
    explicit TBlobReaderCache(TDataNodeConfigPtr config);
    ~TBlobReaderCache();

    typedef TErrorOr<NChunkClient::TFileReaderPtr> TGetReaderResult;

    //! Returns a (cached) blob chunk reader.
    /*!
     *  This call is thread-safe but may block since it actually opens the file.
     *  A rule of thumb is to invoke it from IO thread only.
     *
     *  If chunk file does not exist then |nullptr| is returned.
     *
     *  The returned reader is already open.
     */
    TGetReaderResult GetReader(IChunkPtr chunk);

    //! Evicts the reader from the cache thus hopefully closing the files.
    /*!
     *  NB: Do not make it TChunkPtr since it is called from TCachedBlobChunk dtor.
     */
    void EvictReader(IChunk* chunk);

private:
    class TCachedReader;
    class TImpl;

    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TBlobReaderCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

