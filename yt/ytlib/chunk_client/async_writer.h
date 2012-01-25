#pragma once

#include "common.h"
#include "chunk.pb.h"

#include <ytlib/misc/common.h>
#include <ytlib/misc/enum.h>
#include <ytlib/misc/ref.h>
#include <ytlib/actions/future.h>
#include <ytlib/misc/async_stream_state.h>

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
    : virtual public TRefCounted
{
    typedef TIntrusivePtr<IAsyncWriter> TPtr;

    //! Called when the client wants to upload a new block.
    /*!
     *  Subsequent calls to #AsyncWriteBlock or #AsyncClose are
     *  prohibited until returned result gets set.
     *  If IsOK is true - writer is ready for further work.
     *  IsOK equal to false indicates some error, (e.g. all 
     *  chunk-holders are considered down).
     *  The client shouldn't retry writing the same block again.
     */
    virtual TAsyncError::TPtr AsyncWriteBlock(const TSharedRef& data) = 0;

    //! Called when the client has added all the blocks and is 
    //! willing to finalize the upload.
    /*!
     *  The call completes immediately but returns a result that gets
     *  set when the session is complete.
     *  
     *  Should be called only once.
     *  Calling #AsyncWriteBlock afterwards is an error.
     */
    virtual TAsyncError::TPtr AsyncClose(const NChunkHolder::NProto::TChunkAttributes& attributes) = 0;

    //! Cancels the current upload. 
    //! This method is safe to call at any time.
    /*!
     *  It is safe to call this method at any time and possibly 
     *  multiple times.Calling #AsyncWriteBlock afterwards is an error.
     */
    virtual void Cancel(const TError& error) = 0;

    //! Returns the id of the chunk being written.
    virtual TChunkId GetChunkId() const = 0;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
