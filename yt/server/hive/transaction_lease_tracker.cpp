#include "transaction_lease_tracker.h"

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT {
namespace NHive {

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

void TTransactionLeaseTracker::RegisterTransaction(
    const TTransactionId& transactionId,
    const TTransactionId& parentId,
    TNullable<TDuration> timeout,
    TTransactionLeaseExpirationHandler expirationHandler)
{
    VERIFY_THREAD_AFFINITY_ANY();

    Requests_.Enqueue(TRegisterRequest{
        transactionId,
        parentId,
        timeout,
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

void TTransactionLeaseTracker::PingTransaction(
    const TTransactionId& transactionId,
    bool pingAncestors)
{
    VERIFY_THREAD_AFFINITY(TrackerThread);

    ProcessRequests();

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

void TTransactionLeaseTracker::Reset()
{
    VERIFY_THREAD_AFFINITY_ANY();

    Requests_.Enqueue(TResetRequest{});
}

TFuture<TInstant> TTransactionLeaseTracker::GetLastPingTime(const TTransactionId& transactionId)
{
    return
        BIND([=, this_ = MakeStrong(this)] () {
            VERIFY_THREAD_AFFINITY(TrackerThread);

            auto* descriptor = GetDescriptorOrThrow(transactionId);
            return descriptor->LastPingTime;
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

    if (const auto* registerRequest = request.TryAs<TRegisterRequest>()) {
        ProcessRegisterRequest(*registerRequest);
    } else if (const auto* unregisterRequest = request.TryAs<TUnregisterRequest>()) {
        ProcessUnregisterRequest(*unregisterRequest);
    } else if (const auto* resetRequest = request.TryAs<TResetRequest>()) {
        ProcessResetRequest(*resetRequest);
    } else {
        YUNREACHABLE();
    }
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
    RegisterDeadline(&descriptor);

    LOG_DEBUG("Transaction lease registered (TransactionId: %v)",
        request.TransactionId);
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

void TTransactionLeaseTracker::ProcessResetRequest(const TResetRequest& /*request*/)
{
    IdMap_.clear();
    DeadlineMap_.clear();

    LOG_DEBUG("All transaction leases reset");
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
            NYTree::EErrorCode::ResolveError,
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
    YCHECK(DeadlineMap_.insert(descriptor).second);
}

void TTransactionLeaseTracker::UnregisterDeadline(TTransactionDescriptor* descriptor)
{
    YCHECK(DeadlineMap_.erase(descriptor) == 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
