#pragma once

#include "public.h"

#include <core/misc/ref.h>
#include <core/misc/error.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk_info.pb.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a basic interface for uploading chunks to data nodes.
struct IWriter
    : public virtual TRefCounted
{
    //! Starts a new upload session.
    virtual TAsyncError Open() = 0;

    //! Enqueues another block to be written.
    /*!
     *  If |false| is returned then the block was accepted but the window is already full.
     *  The client must call #GetReadyEvent and wait for the result to be set.
     */
    virtual bool WriteBlock(const TSharedRef& block) = 0;

    //! Similar to #WriteBlock but enqueues a bunch of blocks at once.
    virtual bool WriteBlocks(const std::vector<TSharedRef>& blocks) = 0;

    //! Returns an asynchronous flag used to backpressure the upload.
    virtual TAsyncError GetReadyEvent() = 0;

    //! Called when the client has added all blocks and is
    //! willing to finalize the upload.
    virtual TAsyncError Close(const NChunkClient::NProto::TChunkMeta& chunkMeta) = 0;

    //! Returns the chunk info.
    /*!
     *  This method can only be called when the writer is successfully closed.
     */
    virtual const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const = 0;

    //! Return the indices of replicas that were successfully written.
    /*!
     *  Can only be called when the writer is successfully closed.
     *  Not every writer implements this method.
     */
    virtual TChunkReplicaList GetWrittenChunkReplicas() const = 0;

};

DEFINE_REFCOUNTED_TYPE(IWriter)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
