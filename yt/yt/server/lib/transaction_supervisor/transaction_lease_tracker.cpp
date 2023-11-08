#include "transaction_lease_tracker.h"

#include <yt/yt/server/lib/transaction_server/helpers.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/mpsc_stack.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NTransactionSupervisor {

using namespace NConcurrency;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

class TTransactionLeaseTracker
    : public ITransactionLeaseTracker
{
public:
    TTransactionLeaseTracker(
        IInvokerPtr trackerInvoker,
        const NLogging::TLogger& logger);

    void Start() override;
    void Stop() override;
    void RegisterTransaction(
        TTransactionId transactionId,
        TTransactionId parentId,
        std::optional<TDuration> timeout,
        std::optional<TInstant> deadline,
        TTransactionLeaseExpirationHandler expirationHandler) override;
    void UnregisterTransaction(TTransactionId transactionId) override;
    void SetTimeout(TTransactionId transactionId, TDuration timeout) override;
    void PingTransaction(TTransactionId transactionId, bool pingAncestors = false) override;
    TFuture<TInstant> GetLastPingTime(TTransactionId transactionId) override;

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

    using TRequest = std::variant<
        TStartRequest,
        TStopRequest,
        TRegisterRequest,
        TUnregisterRequest,
        TSetTimeoutRequest
    >;

    TMpscStack<TRequest> Requests_;

    struct TTransactionDescriptor;

    struct TTransactionDeadlineComparer
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
    std::set<TTransactionDescriptor*, TTransactionDeadlineComparer> DeadlineMap_;

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

static const auto TickPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

bool TTransactionLeaseTracker::TTransactionDeadlineComparer::operator()(
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
    YT_VERIFY(TrackerInvoker_);
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
    TTransactionId transactionId,
    TTransactionId parentId,
    std::optional<TDuration> timeout,
    std::optional<TInstant> deadline,
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

void TTransactionLeaseTracker::UnregisterTransaction(TTransactionId transactionId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    Requests_.Enqueue(TUnregisterRequest{
        transactionId
    });
}

void TTransactionLeaseTracker::SetTimeout(
    TTransactionId transactionId,
    TDuration timeout)
{
    VERIFY_THREAD_AFFINITY_ANY();

    Requests_.Enqueue(TSetTimeoutRequest{
        transactionId,
        timeout,
    });
}

void TTransactionLeaseTracker::PingTransaction(
    TTransactionId transactionId,
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

            YT_LOG_DEBUG("Transaction lease renewed (TransactionId: %v)",
                currentId);
        }

        if (!pingAncestors) {
            break;
        }

        currentId = descriptor->ParentId;
    }
}

TFuture<TInstant> TTransactionLeaseTracker::GetLastPingTime(TTransactionId transactionId)
{
    return
        BIND([=, this, this_ = MakeStrong(this)] () {
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

    Visit(request,
        [&] (const TStartRequest& startRequest) {
            ProcessStartRequest(startRequest);
        },
        [&] (const TStopRequest& stopRequest) {
            ProcessStopRequest(stopRequest);
        },
        [&] (const TRegisterRequest& registerRequest) {
            ProcessRegisterRequest(registerRequest);
        },
        [&] (const TUnregisterRequest& unregisterRequest) {
            ProcessUnregisterRequest(unregisterRequest);
        },
        [&] (const TSetTimeoutRequest& setTimeoutRequest) {
            ProcessSetTimeoutRequest(setTimeoutRequest);
        });
}

void TTransactionLeaseTracker::ProcessStartRequest(const TStartRequest& /*request*/)
{
    Active_ = true;

    YT_LOG_INFO("Lease Tracker is active");
}

void TTransactionLeaseTracker::ProcessStopRequest(const TStopRequest& /*request*/)
{
    Active_ = false;
    IdMap_.clear();
    DeadlineMap_.clear();

    YT_LOG_INFO("Lease Tracker is no longer active");
}

void TTransactionLeaseTracker::ProcessRegisterRequest(const TRegisterRequest& request)
{
    auto [it, inserted] = IdMap_.emplace(request.TransactionId, TTransactionDescriptor{
        .TransactionId = request.TransactionId,
        .ParentId = request.ParentId,
        .Timeout = request.Timeout,
        .UserDeadline = request.Deadline,
        .ExpirationHandler = request.ExpirationHandler
    });
    YT_VERIFY(inserted);

    RegisterDeadline(&it->second);

    YT_LOG_DEBUG("Transaction lease registered (TransactionId: %v, Timeout: %v, Deadline: %v)",
        request.TransactionId,
        request.Timeout,
        request.Deadline);
}

void TTransactionLeaseTracker::ProcessUnregisterRequest(const TUnregisterRequest& request)
{
    auto it = IdMap_.find(request.TransactionId);
    if (it == IdMap_.end()) {
        YT_LOG_DEBUG("Requested to unregister non-existent transaction lease, ignored (TransactionId: %v)",
            request.TransactionId);
        return;
    }

    auto* descriptor = &it->second;
    if (!descriptor->TimedOut) {
        UnregisterDeadline(descriptor);
    }
    IdMap_.erase(it);

    YT_LOG_DEBUG("Transaction lease unregistered (TransactionId: %v)",
        request.TransactionId);
}

void TTransactionLeaseTracker::ProcessSetTimeoutRequest(const TSetTimeoutRequest& request)
{
    VERIFY_THREAD_AFFINITY(TrackerThread);

    ValidateActive();

    if (auto descriptor = FindDescriptor(request.TransactionId)) {
        descriptor->Timeout = request.Timeout;

        YT_LOG_DEBUG("Transaction timeout set (TransactionId: %v, Timeout: %v)",
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

        YT_LOG_DEBUG("Transaction lease expired (TransactionId: %v)",
            descriptor->TransactionId);

        descriptor->TimedOut = true;
        descriptor->ExpirationHandler.Run(descriptor->TransactionId);
        DeadlineMap_.erase(it);
    }
}

TTransactionLeaseTracker::TTransactionDescriptor* TTransactionLeaseTracker::FindDescriptor(TTransactionId transactionId)
{
    VERIFY_THREAD_AFFINITY(TrackerThread);

    auto it = IdMap_.find(transactionId);
    return it == IdMap_.end() ? nullptr : &it->second;
}

TTransactionLeaseTracker::TTransactionDescriptor* TTransactionLeaseTracker::GetDescriptorOrThrow(TTransactionId transactionId)
{
    VERIFY_THREAD_AFFINITY(TrackerThread);

    auto* descriptor = FindDescriptor(transactionId);
    if (!descriptor) {
        ThrowNoSuchTransaction(transactionId);
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
        descriptor->Deadline = std::min(descriptor->Deadline, *descriptor->UserDeadline);
    }
    YT_VERIFY(DeadlineMap_.insert(descriptor).second);
}

void TTransactionLeaseTracker::UnregisterDeadline(TTransactionDescriptor* descriptor)
{
    YT_VERIFY(DeadlineMap_.erase(descriptor) == 1);
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

ITransactionLeaseTrackerPtr CreateTransactionLeaseTracker(IInvokerPtr trackerInvoker, const NLogging::TLogger& logger)
{
    return New<TTransactionLeaseTracker>(std::move(trackerInvoker), logger);
}

////////////////////////////////////////////////////////////////////////////////

class TNullTransactionLeaseTracker
    : public ITransactionLeaseTracker
{
public:
    virtual void Start() override
    { }

    virtual void Stop() override
    { }

    virtual void RegisterTransaction(
        TTransactionId /*transactionId*/,
        TTransactionId /*parentId*/,
        std::optional<TDuration> /*timeout*/,
        std::optional<TInstant> /*deadline*/,
        TTransactionLeaseExpirationHandler /*expirationHandler*/) override
    { }

    virtual void UnregisterTransaction(TTransactionId /*transactionId*/) override
    { }

    virtual void SetTimeout(TTransactionId /*transactionId*/, TDuration /*timeout*/) override
    { }

    virtual void PingTransaction(TTransactionId /*transactionId*/, bool /*pingAncestors*/) override
    { }

    virtual TFuture<TInstant> GetLastPingTime(TTransactionId /*transactionId*/) override
    {
        return MakeFuture(TInstant::Zero());
    }
};

ITransactionLeaseTrackerPtr CreateNullTransactionLeaseTracker()
{
    return New<TNullTransactionLeaseTracker>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
