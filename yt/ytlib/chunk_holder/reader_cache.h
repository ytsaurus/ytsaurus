#pragma once

#include "common.h"

#include "../chunk_client/file_reader.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TChunk;

//! Manages cached chunk readers.
class TReaderCache
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TReaderCache> TPtr;

    //! Constructs a new instance.
    TReaderCache(const TChunkHolderConfig& config);

    //! Returns a (cached) chunk reader.
    /*!
     *  This call is thread-safe but may block since it actually opens the file.
     *  A rule of thumb is to invoke it from IO thread only.
     *  
     *  If chunk file does not exist then NULL is returned.
     *  
     *  The returned reader is already open.
     */
    NChunkClient::TFileReader::TPtr FindReader(TChunk* chunk);

    //! Evicts the reader from the cache thus hopefully closing the file.
    void EvictReader(TChunk* chunk);

private:
    class TCachedReader;
    class TImpl;

    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

