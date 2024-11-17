#include "transaction_lease_tracker.h"

#include "config.h"

#include <yt/yt/server/lib/transaction_server/helpers.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/mpsc_stack.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NTransactionSupervisor {

using namespace NConcurrency;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

class TTransactionLeaseTrackerThreadPool
    : public ITransactionLeaseTrackerThreadPool
{
public:
    TTransactionLeaseTrackerThreadPool(
        const TString& threadNamePrefix,
        const TTransactionLeaseTrackerConfigPtr& config)
    {
        for (int index = 0; index < config->ThreadCount; ++index) {
            Queues_.push_back(New<TActionQueue>(Format("%v/%v", threadNamePrefix, index)));
        }
    }

    int GetThreadCount() override
    {
        return std::ssize(Queues_);
    }

    IInvokerPtr GetInvoker(int index) override
    {
        return Queues_[index]->GetInvoker();
    }

private:
    std::vector<TActionQueuePtr> Queues_;
};

////////////////////////////////////////////////////////////////////////////////

ITransactionLeaseTrackerThreadPoolPtr CreateTransactionLeaseTrackerThreadPool(
    TString threadNamePrefix,
    TTransactionLeaseTrackerConfigPtr config)
{
    return New<TTransactionLeaseTrackerThreadPool>(
        std::move(threadNamePrefix),
        std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

static constexpr auto TickPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

class TTransactionLeaseTracker
    : public ITransactionLeaseTracker
{
public:
    TTransactionLeaseTracker(
        ITransactionLeaseTrackerThreadPoolPtr threadPool,
        NLogging::TLogger logger)
        : ThreadPool_(std::move(threadPool))
        , Logger(std::move(logger))
        , Shards_(ThreadPool_->GetThreadCount())
    {
        for (int index = 0; index < ThreadPool_->GetThreadCount(); ++index) {
            auto& shard = Shards_[index];
            shard.Index = index;
            shard.PeriodicExecutor = New<TPeriodicExecutor>(
                ThreadPool_->GetInvoker(index),
                BIND(&TTransactionLeaseTracker::OnTick, MakeWeak(this), index),
                TickPeriod);
            shard.PeriodicExecutor->Start();
        }
    }

    void Start() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        for (auto& shard : Shards_) {
            shard.Requests.Enqueue(TStartRequest{});
        }

        YT_LOG_INFO("Lease Tracker started");
    }

    void Stop() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        for (auto& shard : Shards_) {
            shard.Requests.Enqueue(TStopRequest{});
        }

        YT_LOG_INFO("Lease Tracker stopped");
    }

    void RegisterTransaction(
        TTransactionId transactionId,
        TTransactionId parentId,
        std::optional<TDuration> timeout,
        std::optional<TInstant> deadline,
        TTransactionLeaseExpirationHandler expirationHandler) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ShardFromTransactionId(transactionId)->Requests.Enqueue(TRegisterRequest{
            .TransactionId = transactionId,
            .ParentId = parentId,
            .Timeout = timeout,
            .Deadline = deadline,
            .ExpirationHandler = std::move(expirationHandler),
        });
    }

    void UnregisterTransaction(TTransactionId transactionId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ShardFromTransactionId(transactionId)->Requests.Enqueue(TUnregisterRequest{
            .TransactionId = transactionId,
        });
    }

    void SetTimeout(TTransactionId transactionId, TDuration timeout) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ShardFromTransactionId(transactionId)->Requests.Enqueue(TSetTimeoutRequest{
            .TransactionId = transactionId,
            .Timeout = timeout,
        });
    }

    TFuture<void> PingTransaction(TTransactionId transactionId, bool pingAncestors = false) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto promise = NewPromise<void>();
        auto future = promise.ToFuture();
        ShardFromTransactionId(transactionId)->Requests.Enqueue(TPingRequest{
            .TransactionId = transactionId,
            .PingAncestors = pingAncestors,
            .Promise = std::move(promise),
        });
        return future;
    }

    TFuture<TInstant> GetLastPingTime(TTransactionId transactionId) override
    {
        return BIND(&TTransactionLeaseTracker::DoGetLastPingTime, MakeStrong(this))
            .AsyncVia(ThreadPool_->GetInvoker(ShardIndexFromTransactionId(transactionId)))
            .Run(transactionId);
    }

private:
    const ITransactionLeaseTrackerThreadPoolPtr ThreadPool_;
    const NLogging::TLogger Logger;

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

    struct TPingRequest
    {
        TTransactionId TransactionId;
        bool PingAncestors = false;
        TPromise<void> Promise;
    };

    using TRequest = std::variant<
        TStartRequest,
        TStopRequest,
        TRegisterRequest,
        TUnregisterRequest,
        TSetTimeoutRequest,
        TPingRequest
    >;

    struct TTransactionDescriptor;

    struct TTransactionDeadlineComparer
    {
        bool operator()(const TTransactionDescriptor* lhs, const TTransactionDescriptor* rhs) const
        {
            return
                std::tie(lhs->Deadline, lhs->TransactionId) <
                std::tie(rhs->Deadline, rhs->TransactionId);
        }
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

    struct TShard
    {
        int Index;
        NConcurrency::TPeriodicExecutorPtr PeriodicExecutor;
        TMpscStack<TRequest> Requests;
        bool Active = false;
        THashMap<TTransactionId, TTransactionDescriptor> IdMap;
        std::set<TTransactionDescriptor*, TTransactionDeadlineComparer> DeadlineMap;
    };

    std::vector<TShard> Shards_;


    int ShardIndexFromTransactionId(TTransactionId transactionId)
    {
        return THash<TTransactionId>()(transactionId) % Shards_.size();
    }

    TShard* ShardFromTransactionId(TTransactionId transactionId)
    {
        return &Shards_[ShardIndexFromTransactionId(transactionId)];
    }


    void OnTick(int shardIndex)
    {
        auto* shard = &Shards_[shardIndex];
        ProcessRequests(shard);
        ProcessDeadlines(shard);
    }

    void ProcessRequests(TShard* shard)
    {
        auto requests = shard->Requests.DequeueAll();
        for (auto it = requests.rbegin(); it != requests.rend(); ++it) {
            ProcessRequest(shard, *it);
        }
    }

    void ProcessRequest(TShard* shard, const TRequest& request)
    {
        Visit(request,
            [&] (const TStartRequest& startRequest) {
                ProcessStartRequest(shard, startRequest);
            },
            [&] (const TStopRequest& stopRequest) {
                ProcessStopRequest(shard, stopRequest);
            },
            [&] (const TRegisterRequest& registerRequest) {
                ProcessRegisterRequest(shard, registerRequest);
            },
            [&] (const TUnregisterRequest& unregisterRequest) {
                ProcessUnregisterRequest(shard, unregisterRequest);
            },
            [&] (const TSetTimeoutRequest& setTimeoutRequest) {
                ProcessSetTimeoutRequest(shard, setTimeoutRequest);
            },
            [&] (const TPingRequest& pingRequest) {
                ProcessPingRequest(shard, pingRequest);
            });
    }

    void ProcessStartRequest(TShard* shard, const TStartRequest& /*request*/)
    {
        shard->Active = true;
    }

    void ProcessStopRequest(TShard* shard, const TStopRequest& /*request*/)
    {
        shard->Active = false;
        shard->IdMap.clear();
        shard->DeadlineMap.clear();
    }

    void ProcessRegisterRequest(TShard* shard, const TRegisterRequest& request)
    {
        auto [it, inserted] = shard->IdMap.emplace(request.TransactionId, TTransactionDescriptor{
            .TransactionId = request.TransactionId,
            .ParentId = request.ParentId,
            .Timeout = request.Timeout,
            .UserDeadline = request.Deadline,
            .ExpirationHandler = request.ExpirationHandler,
        });
        YT_VERIFY(inserted);

        RegisterDeadline(shard, &it->second);

        YT_LOG_DEBUG("Transaction lease registered (TransactionId: %v, Timeout: %v, Deadline: %v)",
            request.TransactionId,
            request.Timeout,
            request.Deadline);
    }

    void ProcessUnregisterRequest(TShard* shard, const TUnregisterRequest& request)
    {
        auto it = shard->IdMap.find(request.TransactionId);
        if (it == shard->IdMap.end()) {
            YT_LOG_DEBUG("Requested to unregister non-existent transaction lease, ignored (TransactionId: %v)",
                request.TransactionId);
            return;
        }

        auto* descriptor = &it->second;
        if (!descriptor->TimedOut) {
            UnregisterDeadline(shard, descriptor);
        }
        shard->IdMap.erase(it);

        YT_LOG_DEBUG("Transaction lease unregistered (TransactionId: %v)",
            request.TransactionId);
    }

    void ProcessSetTimeoutRequest(TShard* shard, const TSetTimeoutRequest& request)
    {
        ValidateActive(shard);

        if (auto descriptor = FindDescriptor(shard, request.TransactionId)) {
            descriptor->Timeout = request.Timeout;

            YT_LOG_DEBUG("Transaction timeout set (TransactionId: %v, Timeout: %v)",
                request.TransactionId,
                request.Timeout);
        }
    }

    void RenewLease(TShard* shard, TTransactionDescriptor* descriptor)
    {
        if (!descriptor->TimedOut) {
            UnregisterDeadline(shard, descriptor);
            RegisterDeadline(shard, descriptor);

            YT_LOG_DEBUG("Transaction lease renewed (TransactionId: %v)",
                descriptor->TransactionId);
        }
    }

    void ProcessPingRequest(TShard* rootShard, const TPingRequest& request)
    {
        TTransactionDescriptor* rootDescriptor = nullptr;
        try {
            ValidateActive(rootShard);

            rootDescriptor = GetDescriptorOrThrow(rootShard, request.TransactionId);
            RenewLease(rootShard, rootDescriptor);
        } catch (const std::exception& ex) {
            if (request.Promise) {
                request.Promise.Set(TError(ex));
            }
            return;
        }

        if (request.Promise) {
            request.Promise.Set();
        }

        if (!request.PingAncestors) {
            return;
        }

        auto currentId = rootDescriptor->ParentId;
        while (currentId) {
            if (auto* currentShard = ShardFromTransactionId(currentId); currentShard != rootShard) {
                currentShard->Requests.Enqueue(TPingRequest{
                    .TransactionId = currentId,
                });
                break;
            }

            auto* currentDescriptor = FindDescriptor(rootShard, currentId);
            if (!currentDescriptor) {
                break;
            }

            RenewLease(rootShard, currentDescriptor);
            currentId = currentDescriptor->ParentId;
        }
    }

    void ProcessDeadlines(TShard* shard)
    {
        auto now = TInstant::Now();
        while (!shard->DeadlineMap.empty()) {
            auto it = shard->DeadlineMap.begin();
            auto* descriptor = *it;
            if (descriptor->Deadline > now) {
                break;
            }

            YT_LOG_DEBUG("Transaction lease expired (TransactionId: %v)",
                descriptor->TransactionId);

            // NB: it's important to erase deadline before calling handler since
            // handler may want to register this transaction again.
            shard->DeadlineMap.erase(it);
            descriptor->TimedOut = true;
            descriptor->ExpirationHandler.Run(descriptor->TransactionId);
        }
    }

    TInstant DoGetLastPingTime(TTransactionId transactionId)
    {
        auto* shard = ShardFromTransactionId(transactionId);
        // DoGetLastPingTime is called out-of-band, so we need to flush all the outstanding requests.
        ProcessRequests(shard);
        ValidateActive(shard);
        const auto* descriptor = GetDescriptorOrThrow(shard, transactionId);
        return descriptor->LastPingTime;
    }


    TTransactionDescriptor* FindDescriptor(TShard* shard, TTransactionId transactionId)
    {
        auto it = shard->IdMap.find(transactionId);
        return it == shard->IdMap.end() ? nullptr : &it->second;
    }

    TTransactionDescriptor* GetDescriptorOrThrow(TShard* shard, TTransactionId transactionId)
    {
        auto* descriptor = FindDescriptor(shard, transactionId);
        if (!descriptor) {
            ThrowNoSuchTransaction(transactionId);
        }
        return descriptor;
    }


    void RegisterDeadline(TShard* shard, TTransactionDescriptor* descriptor)
    {
        descriptor->LastPingTime = TInstant::Now();
        descriptor->Deadline = descriptor->Timeout
            ? descriptor->LastPingTime + *descriptor->Timeout
            : TInstant::Max();
        if (descriptor->UserDeadline) {
            descriptor->Deadline = std::min(descriptor->Deadline, *descriptor->UserDeadline);
        }
        InsertOrCrash(shard->DeadlineMap, descriptor);
    }

    void UnregisterDeadline(TShard* shard, TTransactionDescriptor* descriptor)
    {
        EraseOrCrash(shard->DeadlineMap, descriptor);
    }


    void ValidateActive(TShard* shard)
    {
        if (!shard->Active) {
            THROW_ERROR_EXCEPTION(
                NYT::NRpc::EErrorCode::Unavailable,
                "Lease Tracker is not active");
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TTransactionLeaseTracker)

////////////////////////////////////////////////////////////////////////////////

ITransactionLeaseTrackerPtr CreateTransactionLeaseTracker(
    ITransactionLeaseTrackerThreadPoolPtr threadPool,
    NLogging::TLogger logger)
{
    return New<TTransactionLeaseTracker>(
        std::move(threadPool),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

class TNullTransactionLeaseTracker
    : public ITransactionLeaseTracker
{
public:
    void Start() override
    { }

    void Stop() override
    { }

    virtual void RegisterTransaction(
        TTransactionId /*transactionId*/,
        TTransactionId /*parentId*/,
        std::optional<TDuration> /*timeout*/,
        std::optional<TInstant> /*deadline*/,
        TTransactionLeaseExpirationHandler /*expirationHandler*/) override
    { }

    void UnregisterTransaction(TTransactionId /*transactionId*/) override
    { }

    void SetTimeout(TTransactionId /*transactionId*/, TDuration /*timeout*/) override
    { }

    TFuture<void> PingTransaction(TTransactionId /*transactionId*/, bool /*pingAncestors*/) override
    {
        return VoidFuture;
    }

    TFuture<TInstant> GetLastPingTime(TTransactionId /*transactionId*/) override
    {
        return MakeFuture(TInstant::Zero());
    }
};

////////////////////////////////////////////////////////////////////////////////

ITransactionLeaseTrackerPtr CreateNullTransactionLeaseTracker()
{
    return New<TNullTransactionLeaseTracker>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
