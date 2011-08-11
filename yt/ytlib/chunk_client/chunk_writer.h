#pragma once

#include "../misc/common.h"
#include "../misc/ptr.h"
#include "../actions/async_result.h"

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

// TODO: write about error handling
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
     *  This call returns true if the block is added to the queue. Otherwise it returns false
     *  indicating a queue overflow. It also fills #ready with a reference to a result
     *  that gets set when a free queue slot becomes available. The client must subscribe
     *  to the latter result and retry when it is set.
     */
    virtual bool _AddBlock(const TSharedRef& data, TAsyncResult<TVoid>::TPtr* ready)
    {
        UNUSED(data);
        UNUSED(ready);
        return true;
    }

    // TODO: replace with async AddBlock
    virtual void AddBlock(const TSharedRef& data) = 0;

    //! Called when the client has added all the blocks and is willing to
    //! finalize the upload.
    /*!
     *  The call completes immediately but returns a result that gets set
     *  when the session is complete.
     */
    virtual TAsyncResult<TVoid>::TPtr _Close()
    {
        return NULL;
    }

    // TODO: replace with async Close
    virtual void Close() = 0;


    // TODO: implement
    //! Cancels upload.
    virtual void _Cancel()
    {
    }
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
