#pragma once

#include "public.h"

#include <core/misc/ref.h>
#include <core/misc/error.h>

#include <ytlib/chunk_client/chunk.pb.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a basic interface for uploading chunks to data nodes.
struct IAsyncWriter
    : public virtual TRefCounted
{
    //! Starts a new upload session.
    virtual void Open() = 0;

    virtual bool WriteBlock(const TSharedRef& block) = 0;
    virtual TAsyncError GetReadyEvent() = 0;

    //! Called when the client has added all blocks and is
    //! willing to finalize the upload.
    /*!
     *  The call completes immediately but returns a result that gets
     *  set when the session is complete.
     *
     *  Should be called only once.
     *  Calling #WriteBlock afterwards is an error.
     */
    virtual TAsyncError AsyncClose(const NChunkClient::NProto::TChunkMeta& chunkMeta) = 0;

    //! Returns the chunk info.
    /*!
     *  This method can only be called when the writer is successfully closed.
     *
     * \note Thread affinity: ClientThread.
     */
    virtual const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const = 0;

    //! Return indices of alive nodes.
    /*!
     * Can only be called when the writer is successfully closed.
     *
     * This method used in replictaion writer and unimplemented in other cases.
     */
    virtual const std::vector<int> GetWrittenIndexes() const = 0;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
