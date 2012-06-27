#pragma once

#include "public.h"

#include <ytlib/actions/signal.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Represents a transaction within a client.
struct ITransaction
    : public virtual TRefCounted
{
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
     *  If #wait is False then then call does not block and does not throw.
     *  
     *  If #wait is True then the call blocks until the master has confirmed
     *  transaction abort. It may also throw an exception if something goes wrong.
     *  
     *  Safe to call multiple times.
     * 
     *  \note Thread affinity: any
     */
    virtual void Abort(bool wait = false) = 0;

    //! Detaches the transaction, i.e. makes the manager forget about it.
    /*!
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

    //! Specifies is it needed to ping ancestors
    /*!
     *  \note Thread affinity: any
     */
    virtual bool GetPingAncestors() const = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
