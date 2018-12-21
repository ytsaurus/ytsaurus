#pragma once

#include "public.h"

#include <yt/core/actions/callback.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/variant.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

using TTransactionLeaseExpirationHandler = TCallback<void(TTransactionId)>;

//! Offloads the automaton thread by handling transaction pings and leases
//! in a separate thread.
/*!
 *  The instance is active between #Start and #Stop calls, which must come in pairs.
 */
class TTransactionLeaseTracker
    : public TRefCounted
{
public:
    TTransactionLeaseTracker(
        IInvokerPtr trackerInvoker,
        const NLogging::TLogger& logger);


    //! Starts the instance, enables it to serve ping requests.
    /*!
     *  Thread affinity: any
     */
    void Start();

    //! Stops the instance, makes the instance forget about all leases.
    /*!
     *  Thread affinity: any
     */
    void Stop();

    //! Registers a new transaction.
    /*!
     *  Note that calling #RegisterTransaction is allowed even when the instance is not active.
     *  Moreover, these calls typically happen to initialize all relevant leases upon leader startup.
     *
     *  Thread affinity: any
     */
    void RegisterTransaction(
        TTransactionId transactionId,
        TTransactionId parentId,
        std::optional<TDuration> timeout,
        std::optional<TInstant> deadline,
        TTransactionLeaseExpirationHandler expirationHandler);

    //! Unregisters a transaction.
    /*!
     *  This method can only be called when the instance is active.
     *
     *  Thread affinity: any
     */
    void UnregisterTransaction(TTransactionId transactionId);

    //! Sets the transaction timeout. Current lease is not renewed.
    /*!
     *  This method can only be called when the instance is active.
     *
     *  Thread affinity: any
     */
    void SetTimeout(TTransactionId transactionId, TDuration timeout);

    //! Pings a transaction, i.e. renews its lease.
    /*!
     *  When it is not active, throws an error with #NYT::EErrorCode::NRpc::Unavailable code.
     *  Also throws if no transaction with a given #transactionId exists.
     *
     *  Optionally also pings all ancestor transactions.
     *
     *  Thread affinity: TrackerThread
     */
    void PingTransaction(TTransactionId transactionId, bool pingAncestors = false);

    //! Asynchronously returns the (approximate) moment when transaction with
    //! a given #transactionId was last pinged.
    /*!
     *  If the instance is not active, an error with #NYT::EErrorCode::NRpc::Unavailable code is returned.
     *
     *  Thread affinity: any
     */
    TFuture<TInstant> GetLastPingTime(TTransactionId transactionId);

private:
    const IInvokerPtr TrackerInvoker_;
    const NLogging::TLogger Logger;

    const NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    struct TStartRequest
    { };

    struct TStopRequest
    { };

    struct TRegisterRequest
    {
        TTransactionId TransactionId;
        TTransactionId ParentId;
        std::optional<TDuration> Timeout;
        std::optional<TInstant> Deadline;
        TTransactionLeaseExpirationHandler ExpirationHandler;
    };

    struct TUnregisterRequest
    {
        TTransactionId TransactionId;
    };

    struct TSetTimeoutRequest
    {
        TTransactionId TransactionId;
        TDuration Timeout;
    };

    using TRequest = TVariant<
        TStartRequest,
        TStopRequest,
        TRegisterRequest,
        TUnregisterRequest,
        TSetTimeoutRequest
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
        std::optional<TDuration> Timeout;
        std::optional<TInstant> UserDeadline;
        TTransactionLeaseExpirationHandler ExpirationHandler;
        TInstant Deadline;
        TInstant LastPingTime;
        bool TimedOut = false;
    };

    bool Active_ = false;
    THashMap<TTransactionId, TTransactionDescriptor> IdMap_;
    std::set<TTransactionDescriptor*, TTransationDeadlineComparer> DeadlineMap_;

    void OnTick();
    void ProcessRequests();
    void ProcessRequest(const TRequest& request);
    void ProcessStartRequest(const TStartRequest& request);
    void ProcessStopRequest(const TStopRequest& request);
    void ProcessRegisterRequest(const TRegisterRequest& request);
    void ProcessUnregisterRequest(const TUnregisterRequest& request);
    void ProcessSetTimeoutRequest(const TSetTimeoutRequest& request);
    void ProcessDeadlines();

    TTransactionDescriptor* FindDescriptor(TTransactionId transactionId);
    TTransactionDescriptor* GetDescriptorOrThrow(TTransactionId transactionId);

    void RegisterDeadline(TTransactionDescriptor* descriptor);
    void UnregisterDeadline(TTransactionDescriptor* descriptor);

    void ValidateActive();

    DECLARE_THREAD_AFFINITY_SLOT(TrackerThread);

};

DEFINE_REFCOUNTED_TYPE(TTransactionLeaseTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
