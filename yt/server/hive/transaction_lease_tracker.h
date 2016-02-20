#pragma once

#include "public.h"

#include <yt/core/actions/callback.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/variant.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

using TTransactionLeaseExpirationHandler = TCallback<void(const TTransactionId&)>;

//! Offloads the automaton thread by handling transaction pings and leases
//! in a separate thread.
class TTransactionLeaseTracker
    : public TRefCounted
{
public:
    TTransactionLeaseTracker(
        IInvokerPtr trackerInvoker,
        const NLogging::TLogger& logger);

    //! Registers a new transaction.
    /*!
     *  Thread affinity: any
     */
    void RegisterTransaction(
        const TTransactionId& transactionId,
        const TTransactionId& parentId,
        TNullable<TDuration> timeout,
        TTransactionLeaseExpirationHandler expirationHandler);

    //! Registers a new transaction.
    /*!
     *  Thread affinity: any
     */
    void UnregisterTransaction(const TTransactionId& transactionId);

    //! Pings a transaction, i.e. renews its lease.
    /*!
     *  Throws if no transaction with a given #transactionId exists.
     *  Optionally also pings all ancestor transactions.
     *
     *  Thread affinity: TrackerThread
     */
    void PingTransaction(const TTransactionId& transactionId, bool pingAncestors = false);

    //! Makes the instance forget about all transactions and their leases.
    /*!
     *  Thread affinity: any
     */
    void Reset();

    //! Asynchronously returns the (approximate) moment when transaction with
    //! a given #transactionId was last pinged.
    /*!
     *  Thread affinity: any
     */
    TFuture<TInstant> GetLastPingTime(const TTransactionId& transactionId);

private:
    const IInvokerPtr TrackerInvoker_;
    const NLogging::TLogger Logger;

    const NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    struct TRegisterRequest
    {
        TTransactionId TransactionId;
        TTransactionId ParentId;
        TNullable<TDuration> Timeout;
        TTransactionLeaseExpirationHandler ExpirationHandler;
    };

    struct TUnregisterRequest
    {
        TTransactionId TransactionId;
    };

    struct TResetRequest
    { };

    using TRequest = TVariant<
        TRegisterRequest,
        TUnregisterRequest,
        TResetRequest
    >;

    TMultipleProducerSingleConsumerLockFreeStack<TRequest> Requests_;

    struct TTransactionDescriptor;

    struct TTransationDeadlineComparer
    {
        bool operator()(const TTransactionDescriptor* lhs, const TTransactionDescriptor* rhs) const;
    };

    struct TTransactionDescriptor
    {
        TTransactionId TransactionId;
        TTransactionId ParentId;
        TNullable<TDuration> Timeout;
        TTransactionLeaseExpirationHandler ExpirationHandler;
        TInstant Deadline;
        TInstant LastPingTime;
        bool TimedOut = false;
    };

    yhash_map<TTransactionId, TTransactionDescriptor> IdMap_;
    std::set<TTransactionDescriptor*, TTransationDeadlineComparer> DeadlineMap_;

    void OnTick();
    void ProcessRequests();
    void ProcessRequest(const TRequest& request);
    void ProcessRegisterRequest(const TRegisterRequest& request);
    void ProcessUnregisterRequest(const TUnregisterRequest& request);
    void ProcessResetRequest(const TResetRequest& request);
    void ProcessDeadlines();

    TTransactionDescriptor* FindDescriptor(const TTransactionId& transactionId);
    TTransactionDescriptor* GetDescriptorOrThrow(const TTransactionId& transactionId);

    void RegisterDeadline(TTransactionDescriptor* descriptor);
    void UnregisterDeadline(TTransactionDescriptor* descriptor);

    DECLARE_THREAD_AFFINITY_SLOT(TrackerThread);

};

DEFINE_REFCOUNTED_TYPE(TTransactionLeaseTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
