#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/logging/log.h>

#include <optional>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

using TTransactionLeaseExpirationHandler = TCallback<void(TTransactionId)>;

//! Offloads the automaton thread by handling transaction pings and leases
//! in a separate thread.
/*!
 *  The instance is active between #Start and #Stop calls, which must come in pairs.
 */
struct ITransactionLeaseTracker
    : public TRefCounted
{
    // NB: all comments below are related to the implementation produced by
    // #CreateTransactionLeaseTracker. The implementation from #CreateNullTransactionLeaseTracker
    // is effectively no-op for each call.

    //! Starts the instance, enables it to serve ping requests.
    /*!
     *  Thread affinity: any
     */
    virtual void Start() = 0;

    //! Stops the instance, makes the instance forget about all leases.
    /*!
     *  Thread affinity: any
     */
    virtual void Stop() = 0;

    //! Registers a new transaction.
    /*!
     *  Note that calling #RegisterTransaction is allowed even when the instance is not active.
     *  Moreover, these calls typically happen to initialize all relevant leases upon leader startup.
     *
     *  Thread affinity: any
     */
    virtual void RegisterTransaction(
        TTransactionId transactionId,
        TTransactionId parentId,
        std::optional<TDuration> timeout,
        std::optional<TInstant> deadline,
        TTransactionLeaseExpirationHandler expirationHandler) = 0;

    //! Unregisters a transaction.
    /*!
     *  This method can only be called when the instance is active.
     *
     *  Thread affinity: any
     */
    virtual void UnregisterTransaction(TTransactionId transactionId) = 0;

    //! Sets the transaction timeout. Current lease is not renewed.
    /*!
     *  This method can only be called when the instance is active.
     *
     *  Thread affinity: any
     */
    virtual void SetTimeout(TTransactionId transactionId, TDuration timeout) = 0;

    //! Pings a transaction, i.e. renews its lease.
    /*!
     *  When it is not active, throws an error with #NYT::EErrorCode::NRpc::Unavailable code.
     *  Also throws if no transaction with a given #transactionId exists.
     *
     *  Optionally also pings all ancestor transactions.
     *
     *  Thread affinity: TrackerThread
     */
    virtual void PingTransaction(TTransactionId transactionId, bool pingAncestors = false) = 0;

    //! Asynchronously returns the (approximate) moment when transaction with
    //! a given #transactionId was last pinged.
    /*!
     *  If the instance is not active, an error with #NYT::EErrorCode::NRpc::Unavailable code is returned.
     *
     *  Thread affinity: any
     */
    virtual TFuture<TInstant> GetLastPingTime(TTransactionId transactionId) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITransactionLeaseTracker)

////////////////////////////////////////////////////////////////////////////////

//! An implementation providing the behavior described in the interface.
ITransactionLeaseTrackerPtr CreateTransactionLeaseTracker(
    IInvokerPtr trackerInvoker,
    const NLogging::TLogger& logger);

//! An no-op implmemntation. Useful for testing.
ITransactionLeaseTrackerPtr CreateNullTransactionLeaseTracker();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
