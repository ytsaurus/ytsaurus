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
     *  This call may block.
     *  Throws an exception if the commit fails.
     *  Should not be called more than once.
     * 
     *  \note Thread affinity: ClientThread
     */
    virtual void Commit() = 0;

    //! Aborts the transaction.
    /*!
     *  
     *  This call does not block (by default) and does not throw.
     *  When calling with #wait = true this call is blocking
     *  Safe to call multiple times.
     * 
     *  \note Thread affinity: any
     */
    virtual void Abort(bool wait = false) = 0;

    //! Detaches the transaction, i.e. makes the manager forget about it.
    /*!
     *  
     *  This call does not block and does not throw.
     *  Safe to call multiple times.
     * 
     *  \note Thread affinity: ClientThread
     */
    virtual void Detach() = 0;

    //! Returns the id of the transaction.
    /*!
     *  \note Thread affinity: any
     */
    virtual TTransactionId GetId() const  = 0;

    //! Returns the id of the parent transaction.
    /*!
     *  \note Thread affinity: any
     */
    virtual TTransactionId GetParentId() const = 0;

    //! Raised when the transaction is aborted.
    /*!
     *  \note Thread affinity: any
     */
    DECLARE_INTERFACE_SIGNAL(void(), Aborted);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
