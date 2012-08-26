#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_holder_service_proxy.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Manages cached chunk readers.
class TReaderCache
    : public TRefCounted
{
public:
    //! Constructs a new instance.
    TReaderCache(TDataNodeConfigPtr config);

    ~TReaderCache();

    typedef TValueOrError<NChunkClient::TFileReaderPtr> TGetReaderResult;
    typedef NChunkClient::TChunkHolderServiceProxy::EErrorCode EErrorCode;

    //! Returns a (cached) chunk reader.
    /*!
     *  This call is thread-safe but may block since it actually opens the file.
     *  A rule of thumb is to invoke it from IO thread only.
     *  
     *  If chunk file does not exist then NULL is returned.
     *  
     *  The returned reader is already open.
     */
     TGetReaderResult GetReader(TChunkPtr chunk);

    //! Evicts the reader from the cache thus hopefully closing the file.
    void EvictReader(TChunkPtr chunk);

private:
    class TCachedReader;
    class TImpl;

    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

