#pragma once

#include <ytlib/chunk_holder/chunk.pb.h>

#include <ytlib/misc/common.h>
#include <ytlib/misc/ref.h>
#include <ytlib/misc/error.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a basic interface for uploading chunks to holders.
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
     *  Subsequent calls to #AsyncWriteBlock or #AsyncClose are
     *  prohibited until the returned result is set.
     *  If the result indicates some error then the whole upload session is failed.
     *  (e.g. all chunk-holders are down).
     *  The client shouldn't retry writing the same block again.
     */
    virtual TAsyncError AsyncWriteBlocks(const std::vector<TSharedRef>& blocks) = 0;

    //! Called when the client has added all blocks and is 
    //! willing to finalize the upload.
    /*!
     *  The call completes immediately but returns a result that gets
     *  set when the session is complete.
     *  
     *  Should be called only once.
     *  Calling #AsyncWriteBlock afterwards is an error.
     */
    virtual TAsyncError AsyncClose(const NChunkHolder::NProto::TChunkMeta& chunkMeta) = 0;

    //! Returns the chunk info.
    /*!
     *  This method can only be called when the writer is successfully closed.
     *  
     * \note Thread affinity: ClientThread.
     */
    virtual const NChunkHolder::NProto::TChunkInfo& GetChunkInfo() const = 0;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
