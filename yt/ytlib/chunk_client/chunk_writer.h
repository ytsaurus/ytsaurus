#pragma once

#include "../misc/common.h"
#include "../misc/assert.h"
#include "../misc/enum.h"
#include "../misc/ptr.h"
#include "../actions/future.h"

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

    DECLARE_ENUM(EResult,
        (OK)
        (TryLater)
        (Failed)
    );

    //! Called when the client wants to upload a new block.
    /*!
     *  This call returns OK if the block is added to the queue. Otherwise it returns TryLater
     *  indicating a queue overflow or Failed if all chunk-holders are considered down. 
     *  It also fills #ready with a reference to a result that gets set when a free queue 
     *  slot becomes available. The client must subscribe to the latter result and should 
     *  not retry before it is set.
     */
    virtual EResult AsyncWriteBlock(
        const TSharedRef& data,
        TFuture<TVoid>::TPtr* ready) = 0;

    //! Called when the client has added all the blocks and is willing to
    //! finalize the upload.
    /*!
     *  The call completes immediately but returns a result that gets set
     *  when the session is complete. Result may contain OK if write was
     *  completed successfully, otherwise Failed.
     *  
     *  It is safe to call this method at any time and possibly multiple times.
     *  Calling #AsyncWriteBlock afterwards is an error.
     */
    virtual TFuture<EResult>::TPtr AsyncClose() = 0;

    //! A synchronous version of #AsyncAddBlock, throws an exception if uploading fails.
    void WriteBlock(const TSharedRef& data)
    {
        while (true) {
            TFuture<TVoid>::TPtr ready;
            EResult result = AsyncWriteBlock(data, &ready);
            CheckResult(result);
            switch (result) {
                case EResult::OK:
                    return;

                case EResult::TryLater:
                    ready->Get();
                    break;

                default:
                    YUNREACHABLE();
                    break;
            }
        }
    }

    //! A synchronous version of #AsyncClose, throws an exception if uploading fails
    void Close()
    {
        EResult result = AsyncClose()->Get();
        CheckResult(result);
        YASSERT(result == EResult::OK);
    }

    //! Cancels the current upload. This method is safe to call at any time.
    /*!
     *  It is safe to call this method at any time and possibly multiple times.
     *  Calling #AsyncWriteBlock afterwards is an error.
     */
    virtual void Cancel() = 0;

private:
    void CheckResult(EResult result)
    {
        if (result == EResult::Failed) {
            ythrow yexception() << "Chunk writing failed";
        }
    }

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
