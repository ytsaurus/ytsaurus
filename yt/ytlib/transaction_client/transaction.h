#pragma once

#include "public.h"

#include <core/actions/signal.h>

#include <core/rpc/public.h>

#include <ytlib/meta_state/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Represents a transaction within a client.
struct ITransaction
    : public virtual TRefCounted
{
    //! Commits the transaction asynchronously.
    /*!
     *  \note Thread affinity: ClientThread
     */
    virtual TAsyncError AsyncCommit(
        const NMetaState::TMutationId& mutationId = NMetaState::NullMutationId) = 0;

    //! Commits the transaction synchronously.
    /*!
     *  This call may block.
     *  Throws an exception if the commit fails.
     *  Should not be called more than once.
     *
     *  \note Thread affinity: ClientThread
     */
    virtual void Commit(
        const NMetaState::TMutationId& mutationId = NMetaState::NullMutationId) = 0;

    //! Aborts the transaction asynchronously.
    virtual TAsyncError AsyncAbort(
        bool generateMutationId,
        const NMetaState::TMutationId& mutationId = NMetaState::NullMutationId) = 0;

    //! Aborts the transaction synchronously.
    /*!
     *
     *  If #wait is False then then call does not block and does not throw.
     *
     *  If #wait is true then the call blocks until the master has confirmed
     *  transaction abort. It may also throw an exception if something goes wrong.
     *
     *  Safe to call multiple times.
     *
     *  \note Thread affinity: any
     */
    virtual void Abort(
        bool wait = false,
        const NMetaState::TMutationId& mutationId = NMetaState::NullMutationId) = 0;

    //! Detaches the transaction, i.e. makes the manager forget about it.
    /*!
     *  This call does not block and does not throw.
     *  Safe to call multiple times.
     *
     *  \note Thread affinity: ClientThread
     */
    virtual void Detach() = 0;

    //! Sends an asynchronous ping.
    /*!
     *  \note Thread affinity: any
     */
    virtual TAsyncError AsyncPing() = 0;

    //! Returns the id of the transaction.
    /*!
     *  \note Thread affinity: any
     */
    virtual TTransactionId GetId() const  = 0;

    //! Raised when the transaction is aborted.
    /*!
     *  \note Thread affinity: any
     */
    DECLARE_INTERFACE_SIGNAL(void(), Aborted);
};

////////////////////////////////////////////////////////////////////////////////

//! Attaches transaction id to the given request.
/*!
 *  #transaction may be null.
 */
void SetTransactionId(NRpc::IClientRequestPtr request, ITransactionPtr transaction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
