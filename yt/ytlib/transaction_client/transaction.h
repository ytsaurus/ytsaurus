#pragma once

#include "public.h"

#include <core/actions/signal.h>

#include <core/rpc/public.h>

#include <ytlib/hydra/public.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Represents a transaction within a client.
struct ITransaction
    : public virtual TRefCounted
{
    //! Commits the transaction asynchronously.
    /*!
     *  Should not be called more than once.
     *
     *  \note Thread affinity: ClientThread
     */
    virtual TAsyncError AsyncCommit(const NHydra::TMutationId& mutationId = NHydra::NullMutationId) = 0;

    //! Aborts the transaction asynchronously.
    virtual TAsyncError AsyncAbort(const NHydra::TMutationId& mutationId = NHydra::NullMutationId) = 0;

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

    //! Returns the transaction id.
    /*!
     *  \note Thread affinity: any
     */
    virtual TTransactionId GetId() const  = 0;

    //! Returns the transaction start timestamp.
    /*!
     *  \note Thread affinity: any
     */
    virtual TTimestamp GetStartTimestamp() const  = 0;

    //! Called to mark a given cell as a transaction participant.
    //! Starts the corresponding transaction in background.
    /*!
     *  \note Thread affinity: ClientThread
     */
    virtual void AddParticipant(const NElection::TCellGuid& cellGuid) = 0;

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
