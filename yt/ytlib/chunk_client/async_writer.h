#pragma once

#include "public.h"

#include <ytlib/misc/ref.h>
#include <ytlib/misc/error.h>

#include <ytlib/chunk_client/chunk.pb.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a basic interface for uploading chunks to data nodes.
/*!
 *  The client must feed the blocks one after another with #AddBlock method.
 *  It must call #Close to finish the session.
 *  An implementation may provide a buffering window (queue) to enable concurrent upload to
 *  multiple destinations using torrent or chaining strategies.
 */
struct IAsyncWriter
    : public virtual TRefCounted
{
    //! Starts a new upload session.
    virtual void Open() = 0;

    //! Called when the client wants to upload a new block.
    /*!
     *  Subsequent calls to #TryWriteBlock or #AsyncClose are
     *  prohibited until the returned result is set.
     *
     *  Returns false if error detected.
     *
     *  Call #GetReadyEvent to obtain the result.
     *  If the result indicates some error then the whole upload session is failed.
     *  (e.g. all target data nodes are down).
     *  The client must not retry and send the same block again.
     */
    virtual bool TryWriteBlock(const TSharedRef& block) = 0;
    virtual TAsyncError GetReadyEvent() = 0;

    //! Called when the client has added all blocks and is
    //! willing to finalize the upload.
    /*!
     *  The call completes immediately but returns a result that gets
     *  set when the session is complete.
     *
     *  Should be called only once.
     *  Calling #AsyncWriteBlock afterwards is an error.
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
     * Can only be called when the wrtier is successfully closed.
     *
     * This method used in replictaion writer and unimplemented in other cases.
     */
    virtual const std::vector<int> GetWrittenIndexes() const = 0;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
