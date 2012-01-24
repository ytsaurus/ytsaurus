#pragma once

#include "common.h"

#include <ytlib/actions/action.h>

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
     *  Does not throw.
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

    virtual void SubscribeAborted(IAction::TPtr onAborted) = 0;
    virtual void UnsubscribeAborted(IAction::TPtr onAborted) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
