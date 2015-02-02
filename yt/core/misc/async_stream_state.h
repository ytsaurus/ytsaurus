#pragma once

#include <core/misc/error.h>
#include <core/actions/future.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Manages the internal state of async input and output streams, e.g.
//! #TRemoteChunkWriter, #TSequentialChunkReader and many #NTableClient classes.
class TAsyncStreamState
    : private TNonCopyable
{
public:
    TAsyncStreamState();

    /*!
     *  If the stream has already failed, the result indicates the error.
     *  Otherwise (the stream active is already closed) the result is OK.
     */
    const TError& GetCurrentError();

    //! Moves stream to failed state if it is active.
    /*!
     *  Can be called multiple times.
     *  Has no guarantees and cannot fail.
     *  If stream is already closed, failed or canceled -
     *  this call does nothing.
     */
    void Cancel(const TError& error);

    //! Moves stream to failed state. Stream should be active.
    /*!
     *  Can be called multiple times.
     *  If stream is successfully closed - fails on assert.
     */
    void Fail(const TError& error);

    //! Moves stream to closed state.
    /*!
     *  Can be called only once, stream should be active.
     *  Otherwise fails on assert.
     */
    void Close();

    //! Invokes #Close or #Fail depending on #error.
    void Finish(const TError& error);

    //! Returns if the stream is active.
    bool IsActive() const;
    /*!
     *  \note
     *  A stream is considered active iFf it is neither closed nor failed.
     */

    //! Returns if the stream is closed.
    /*!
     *  A stream must be closed by explicitly calling #Close.
     */
    bool IsClosed() const;

    //! The following calls are used to support async operations
    //! that can be sometimes (usually) completed synchronously.
    //! This allows to eliminate excessive creation of futures for operations,
    //! that complete synchronously.
    //! Client must not start new operation until previous is finished.

    //! Called by user before starting async operation.
    void StartOperation();

    bool HasRunningOperation() const;

    //! Returned from async operation.
    /*!
     *  If operation is completed or stream has already failed,
     *  this call returns preliminary prepared future.
     *  Otherwise new future is created.
     */
    TFuture<void> GetOperationError();

    //! Complete operations.
    void FinishOperation(const TError& error = TError());

private:
    void DoFail();

    volatile bool IsOperationFinished;
    volatile bool IsActive_;

    TSpinLock SpinLock;

    //! Result returned from #GetOperationResult when operation is
    //! already completed, or the stream has failed.
    TPromise<void> StaticError;

    //! Last unset future created via #GetOperationResult
    TPromise<void> CurrentError;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
