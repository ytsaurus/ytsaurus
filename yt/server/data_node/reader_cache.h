#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/data_node_service_proxy.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages cached chunk readers.
class TReaderCache
    : public TRefCounted
{
public:
    //! Constructs a new instance.
    explicit TReaderCache(TDataNodeConfigPtr config);
    ~TReaderCache();

    typedef TErrorOr<NChunkClient::TFileReaderPtr> TGetReaderResult;

    //! Returns a (cached) chunk reader.
    /*!
     *  This call is thread-safe but may block since it actually opens the file.
     *  A rule of thumb is to invoke it from IO thread only.
     *
     *  If chunk file does not exist then NULL is returned.
     *
     *  The returned reader is already open.
     */
     TGetReaderResult GetReader(IChunkPtr chunk);

    //! Evicts the reader from the cache thus hopefully closing the file.
    /*!
        NB: do not make it TChunkPtr since it is called from TCachedBlobChunk dtor
    */
    void EvictReader(IChunk* chunk);

private:
    class TCachedReader;
    class TImpl;

    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TReaderCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

