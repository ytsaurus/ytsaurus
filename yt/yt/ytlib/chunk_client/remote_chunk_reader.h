#pragma once

#include "chunk_reader.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! An interface for the chunk reader that reads chunks from remote nodes.
struct IRemoteChunkReader
    : public IChunkReader
{
    //! Synchronously returns the set of chunk replicas currently known to the reader.
    virtual TChunkReplicaList GetReplicas() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRemoteChunkReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
