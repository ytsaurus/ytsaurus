#pragma once

#include "common.h"
#include "../misc/common.h"
#include "../misc/assert.h"
#include "../misc/enum.h"
#include "../misc/ref.h"
#include "../actions/future.h"
#include "../misc/async_stream_state.h"

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

//! Provides a basic interface for uploading chunks to holders.
/*!
 *  The client must feed the blocks one after another with #AddBlock method.
 *  It must call #Close to finish the session.
 *  An implementation may provide a buffering window (queue) to enable concurrent upload to
 *  multiple destinations using torrent or chaining strategies.
 */
struct IChunkWriter
    : virtual public TRefCountedBase
{
    typedef TIntrusivePtr<IChunkWriter> TPtr;

    //! Called when the client wants to upload a new block.
    /*!
     *  Subsequent calls to #AsyncWriteBlock or #AsyncClose are
     *  prohibited until returned result gets set.
     *  If IsOK is true - writer is ready for further work.
     *  IsOK equal to false indicates some error, (e.g. all 
     *  chunk-holders are considered down).
     *  The client shouldn't retry writing the same block again.
     */
    virtual TAsyncStreamState::TAsyncResult::TPtr AsyncWriteBlock(
        const TSharedRef& data) = 0;

    //! Called when the client has added all the blocks and is 
    //! willing to finalize the upload.
    /*!
     *  The call completes immediately but returns a result that gets
     *  set when the session is complete.
     *  
     *  Should be called only once.
     *  Calling #AsyncWriteBlock afterwards is an error.
     */
    virtual TAsyncStreamState::TAsyncResult::TPtr AsyncClose() = 0;

    //! Cancels the current upload. 
    //! This method is safe to call at any time.
    /*!
     *  It is safe to call this method at any time and possibly 
     *  multiple times.Calling #AsyncWriteBlock afterwards is an error.
     */
    virtual void Cancel(const Stroka& errorMessage) = 0;

    const TChunkId& GetChunkId() const;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
