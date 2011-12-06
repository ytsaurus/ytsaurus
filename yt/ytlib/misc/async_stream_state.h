#pragma once

#include "../actions/future.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Manages the internal state of async input and output streams, e.g.
//! #TRemoteChunkWriter, #TSequentialChunkReader and many #NTableClient classes.
class TAsyncStreamState
    : public TNonCopyable
{
public:
    // TODO: replace with TError
    struct TResult
    {
        /*! True means that stream is ready (if set through #Signal)
         *  or successfully closed (#Close). This can be distinguished 
         *  by #IsClosed call.
         *  False means failure or cancellation. Failure details can be obtained
         *  via ErrorMessage.
         */
        bool IsOK;

        //! Detailed information about occurred errors.
        /*!
         * \note Is used as an exception message for sync calls.
         */
        Stroka ErrorMessage;

        TResult(bool isOk = true, const Stroka& errorMessage = "");
    };

    typedef TFuture<TResult> TAsyncResult;

    TAsyncStreamState();

    /*!
     *  IsOK is false if the stream has already failed.
     *  Otherwise (successfully closed or active) - true.
     */
    TResult GetCurrentResult();

    //! Moves stream to failed state if it is active.
    /*!
     *  Can be called multiple times.
     *  Has no guarantees and cannot fail.
     *  If stream is already closed, failed or canceled - 
     *  this call does nothing.
     * 
     *  \param errorMessage - reason of cancellation.
     */
    // TODO: errorMessage -> TError
    void Cancel(const Stroka& errorMessage);

    //! Moves stream to failed state. Stream should be active.
    /*!
     *  Can be called multiple times.
     *  If stream is successfully closed - fails on assert.
     * 
     * \param errorMessage - reason of cancellation.
     */
    // TODO: errorMessage -> TError
    void Fail(const Stroka& errorMessage);

    //! Moves stream to closed state.
    /*!
     *  Can be called only once, stream should be active.
     *  Otherwise fails on assert.
     */
    void Close();

    //! Invokes #Close or #Fail depending on #result.
    // TODO: TResult -> TError
    void Finish(TResult result);

    //! Returns if the stream is active.
    bool IsActive() const;
    /*!
     *  \note
     *  A stream is considered active if it is neither closed nor failed.
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
    TAsyncResult::TPtr GetOperationResult();

    //! Complete operations.
    // TODO: TResult -> TError
    void FinishOperation(TResult result = TResult());

private:
    void DoFail(const Stroka& errorMessage);

    bool IsOperationFinished;
    bool IsActive_;

    TSpinLock SpinLock;

    //! Result returned from #GetOperationResult when operation is
    //! already completed, or stream has failed.
    TAsyncResult::TPtr StaticResult;

    //! Last unset AsyncResult created via #GetOperationResult
    TAsyncResult::TPtr CurrentResult;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
