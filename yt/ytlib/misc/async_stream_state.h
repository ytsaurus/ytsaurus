#pragma once

#include "../actions/future.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Handles the internal state of async input and output streams, e.g.
//! #TRemoteChunkWriter, #TSequentialChunkReader and many #NTableClient classes.
class TAsyncStreamState
    : public TNonCopyable
{
public:
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
    void Cancel(const Stroka& errorMessage);

    //! Moves stream to failed state. Stream should be active.
    /*!
     *  Can be called multiple times.
     *  If stream is successfully closed - fails on assert.
     * 
     * \param errorMessage - reason of cancellation.
     */
    void Fail(const Stroka& errorMessage);

    //! Moves stream to closed state.
    /*!
     *  Can be called only once, stream should be active.
     *  Otherwise fails on assert.
     */
    void Close();

    //! Acts like #Close or #Fail depending on #result.
    void Finish(TResult result);

    bool IsActive() const;
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
    void FinishOperation(TResult result = TResult());
    // void FinishOperation(const Stroka& errorMessage);

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

// ToDo: codegen! Type traits.
struct ISyncInterface
{
    template<class TUnderlying>
    void Sync(
        TAsyncStreamState::TAsyncResult::TPtr (TUnderlying::*method)()) 
    {
        auto result = this->*method();
        if (!result.IsOK) {
            ythrow yexception() << result.ErrorMessage;
        }
    }

    template<class TUnderlying, class TArg1>
    void Sync(
        TAsyncStreamState::TAsyncResult::TPtr (TUnderlying::*method)(const TArg1&), 
        const TArg1& arg1) 
    {
        auto result = (static_cast<TUnderlying*>(this)->*method)(arg1)->Get();
        if (!result.IsOK) {
            ythrow yexception() << result.ErrorMessage;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
