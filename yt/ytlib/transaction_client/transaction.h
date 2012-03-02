#pragma once

#include "common.h"

#include <ytlib/actions/signal.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Represents a transaction within a client.
struct ITransaction
    : public virtual TRefCounted
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
     *  This call does not block and does not throw.
     *  Safe to call multiple times.
     * 
     *  Thread affinity: any.
     */
    virtual void Abort() = 0;

    //! Returns the id of the transaction.
    /*!
     *  \note
     *  Thread affinity: any.
     */
    virtual TTransactionId GetId() const  = 0;

    //! Returns the id of the parent transaction.
    /*!
     *  \note
     *  Thread affinity: any.
     */
    virtual TTransactionId GetParentId() const = 0;

    //! Raised when the transaction is aborted.
    /*!
     *  \note
     *  Thread affinity: any.
     */
    DECLARE_SIGNAL_INTERFACE(IAction, Aborted);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
