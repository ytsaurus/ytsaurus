#pragma once

#include "common.h"

#include "../actions/signal.h"

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Represents a transaction within a client.
struct ITransaction
    : virtual public TRefCountedBase
{
    typedef TIntrusivePtr<ITransaction> TPtr;

    //! Commits the transaction.
    /*!
     * \note
     * This call may block.
     * Throws an exception if the commit fails.
     * Should not be called more than once.
     * 
     * Thread affinity: ClientThread.
     */
    virtual void Commit() = 0;

    //! Aborts the transaction.
    /*!
     *  \note
     *  This call may block.
     *  TODO: exceptions?
     *  Safe to call multiple times.
     * 
     *  Thread affinity: ClientThread.
     */
    virtual void Abort() = 0;

    //! Returns the id of the transaction.
    /*!
     *  \note
     *  Thread affinity: any.
     */
    virtual TTransactionId GetId() const  = 0;


    //! Raised when the transaction is committed successfully.
    /*!
     *  \note
     *  Raised from an unspecified thread.
     *  
     *  Thread affinity: any.
     */
    virtual TSignal& OnCommitted() = 0;

    //! Raised when the transaction is either aborted by the client
    //! or a ping has failed.
    /*!
    *   \note
    *   Raised from an unspecified thread.
    *   
    *   Thread affinity: any.
     */
    virtual TSignal& OnAborted() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
