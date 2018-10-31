#include "transaction_lease_tracker.h"

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT {
namespace NHiveServer {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto TickPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

bool TTransactionLeaseTracker::TTransationDeadlineComparer::operator()(
    const TTransactionDescriptor* lhs,
    const TTransactionDescriptor* rhs) const
{
    return
        std::tie(lhs->Deadline, lhs->TransactionId) <
        std::tie(rhs->Deadline, rhs->TransactionId);
}

////////////////////////////////////////////////////////////////////////////////

TTransactionLeaseTracker::TTransactionLeaseTracker(
    IInvokerPtr trackerInvoker,
    const NLogging::TLogger& logger)
    : TrackerInvoker_(std::move(trackerInvoker))
    , Logger(logger)
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        TrackerInvoker_,
        BIND(&TTransactionLeaseTracker::OnTick, MakeWeak(this)),
        TickPeriod))
{
    YCHECK(TrackerInvoker_);
    VERIFY_INVOKER_THREAD_AFFINITY(TrackerInvoker_, TrackerThread);

    PeriodicExecutor_->Start();
}

void TTransactionLeaseTracker::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();

    Requests_.Enqueue(TStartRequest{});
}

void TTransactionLeaseTracker::Stop()
{
    VERIFY_THREAD_AFFINITY_ANY();

    Requests_.Enqueue(TStopRequest{});
}

void TTransactionLeaseTracker::RegisterTransaction(
    const TTransactionId& transactionId,
    const TTransactionId& parentId,
    TNullable<TDuration> timeout,
    TInstant deadline,
    TTransactionLeaseExpirationHandler expirationHandler)
{
    VERIFY_THREAD_AFFINITY_ANY();

    Requests_.Enqueue(TRegisterRequest{
        transactionId,
        parentId,
        timeout,
        deadline,
        std::move(expirationHandler)
    });
}

void TTransactionLeaseTracker::UnregisterTransaction(const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    Requests_.Enqueue(TUnregisterRequest{
        transactionId
    });
}

void TTransactionLeaseTracker::SetTimeout(
    const TTransactionId& transactionId,
    TDuration timeout)
{
    VERIFY_THREAD_AFFINITY_ANY();

    Requests_.Enqueue(TSetTimeoutRequest{
        transactionId,
        timeout,
    });
}

void TTransactionLeaseTracker::PingTransaction(
    const TTransactionId& transactionId,
    bool pingAncestors)
{
    VERIFY_THREAD_AFFINITY(TrackerThread);

    ProcessRequests();
    ValidateActive();

    auto currentId = transactionId;
    while (true) {
        auto* descriptor = (currentId == transactionId)
            ? GetDescriptorOrThrow(currentId)
            : FindDescriptor(currentId);

        if (!descriptor) {
            break;
        }

        if (!descriptor->TimedOut) {
            UnregisterDeadline(descriptor);
            RegisterDeadline(descriptor);

            LOG_DEBUG("Transaction lease renewed (TransactionId: %v)",
                currentId);
        }

        if (!pingAncestors) {
            break;
        }

        currentId = descriptor->ParentId;
    }
}

TFuture<TInstant> TTransactionLeaseTracker::GetLastPingTime(const TTransactionId& transactionId)
{
    return
        BIND([=, this_ = MakeStrong(this)] () {
            VERIFY_THREAD_AFFINITY(TrackerThread);

            ValidateActive();
            return GetDescriptorOrThrow(transactionId)->LastPingTime;
        })
        .AsyncVia(TrackerInvoker_)
        .Run();
}

void TTransactionLeaseTracker::OnTick()
{
    ProcessRequests();
    ProcessDeadlines();
}

void TTransactionLeaseTracker::ProcessRequests()
{
    auto requests = Requests_.DequeueAll();
    for (auto it = requests.rbegin(); it != requests.rend(); ++it) {
        ProcessRequest(*it);
    }
}

void TTransactionLeaseTracker::ProcessRequest(const TRequest& request)
{
    VERIFY_THREAD_AFFINITY(TrackerThread);

    if (const auto* startRequest = request.TryAs<TStartRequest>()) {
        ProcessStartRequest(*startRequest);
    } else if (const auto* stopRequest = request.TryAs<TStopRequest>()) {
        ProcessStopRequest(*stopRequest);
    } else if (const auto* registerRequest = request.TryAs<TRegisterRequest>()) {
        ProcessRegisterRequest(*registerRequest);
    } else if (const auto* unregisterRequest = request.TryAs<TUnregisterRequest>()) {
        ProcessUnregisterRequest(*unregisterRequest);
    } else if (const auto* setTimeoutRequest = request.TryAs<TSetTimeoutRequest>()) {
        ProcessSetTimeoutRequest(*setTimeoutRequest);
    } else {
        Y_UNREACHABLE();
    }
}

void TTransactionLeaseTracker::ProcessStartRequest(const TStartRequest& /*request*/)
{
    Active_ = true;

    LOG_INFO("Lease Tracker is active");
}

void TTransactionLeaseTracker::ProcessStopRequest(const TStopRequest& /*request*/)
{
    Active_ = false;
    IdMap_.clear();
    DeadlineMap_.clear();

    LOG_INFO("Lease Tracker is no longer active");
}

void TTransactionLeaseTracker::ProcessRegisterRequest(const TRegisterRequest& request)
{
    auto idPair = IdMap_.insert(std::make_pair(request.TransactionId, TTransactionDescriptor()));
    YCHECK(idPair.second);
    auto& descriptor = idPair.first->second;
    descriptor.TransactionId = request.TransactionId;
    descriptor.ParentId = request.ParentId;
    descriptor.ExpirationHandler = request.ExpirationHandler;
    descriptor.Timeout = request.Timeout;
    descriptor.UserDeadline = request.Deadline;
    RegisterDeadline(&descriptor);

    LOG_DEBUG("Transaction lease registered (TransactionId: %v, Timeout: %v, Deadline: %v)",
        request.TransactionId,
        request.Timeout,
        MakeNullable(request.Deadline != TInstant::Zero(), request.Deadline),
        request.Deadline);
}

void TTransactionLeaseTracker::ProcessUnregisterRequest(const TUnregisterRequest& request)
{
    auto it = IdMap_.find(request.TransactionId);
    YCHECK(it != IdMap_.end());
    auto* descriptor = &it->second;
    if (!descriptor->TimedOut) {
        UnregisterDeadline(descriptor);
    }
    IdMap_.erase(it);

    LOG_DEBUG("Transaction lease unregistered (TransactionId: %v)",
        request.TransactionId);
}

void TTransactionLeaseTracker::ProcessSetTimeoutRequest(const TSetTimeoutRequest& request)
{
    VERIFY_THREAD_AFFINITY(TrackerThread);

    ValidateActive();

    if (auto descriptor = FindDescriptor(request.TransactionId)) {
        descriptor->Timeout = request.Timeout;

        LOG_DEBUG("Transaction timeout set (TransactionId: %v, Timeout: %v)",
            request.TransactionId,
            request.Timeout);
    }
}

void TTransactionLeaseTracker::ProcessDeadlines()
{
    VERIFY_THREAD_AFFINITY(TrackerThread);

    auto now = TInstant::Now();
    while (!DeadlineMap_.empty()) {
        auto it = DeadlineMap_.begin();
        auto& descriptor = *it;
        if (descriptor->Deadline > now) {
            break;
        }

        LOG_DEBUG("Transaction lease expired (TransactionId: %v)",
            descriptor->TransactionId);

        descriptor->TimedOut = true;
        descriptor->ExpirationHandler.Run(descriptor->TransactionId);
        DeadlineMap_.erase(it);
    }
}

TTransactionLeaseTracker::TTransactionDescriptor* TTransactionLeaseTracker::FindDescriptor(const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(TrackerThread);

    auto it = IdMap_.find(transactionId);
    return it == IdMap_.end() ? nullptr : &it->second;
}

TTransactionLeaseTracker::TTransactionDescriptor* TTransactionLeaseTracker::GetDescriptorOrThrow(const TTransactionId& transactionId)
{
    VERIFY_THREAD_AFFINITY(TrackerThread);

    auto* descriptor = FindDescriptor(transactionId);
    if (!descriptor) {
        THROW_ERROR_EXCEPTION(
            NTransactionClient::EErrorCode::NoSuchTransaction,
            "No such transaction %v",
            transactionId);
    }
    return descriptor;
}

void TTransactionLeaseTracker::RegisterDeadline(TTransactionDescriptor* descriptor)
{
    descriptor->LastPingTime = TInstant::Now();
    descriptor->Deadline = descriptor->Timeout
        ? descriptor->LastPingTime + *descriptor->Timeout
        : TInstant::Max();
    if (descriptor->UserDeadline) {
        descriptor->Deadline = std::min(descriptor->Deadline, descriptor->UserDeadline);
    }
    YCHECK(DeadlineMap_.insert(descriptor).second);
}

void TTransactionLeaseTracker::UnregisterDeadline(TTransactionDescriptor* descriptor)
{
    YCHECK(DeadlineMap_.erase(descriptor) == 1);
}

void TTransactionLeaseTracker::ValidateActive()
{
    if (!Active_) {
        THROW_ERROR_EXCEPTION(
            NYT::NRpc::EErrorCode::Unavailable,
            "Lease Tracker is not active");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
