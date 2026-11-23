#include "factory.h"

#include <yt/yt/flow/library/cpp/distributed_throttler/prefetching/client.h>

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <library/cpp/yt/system/atomic_object.h>
#include <library/cpp/yt/system/spin_lock.h>

#include <mutex>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TThrottlerWrapper
    : public NConcurrency::IThroughputThrottler
{
    struct TBinding
    {
        NConcurrency::IThroughputThrottlerPtr Underlying;
        TDuration Timeout;
    };

public:
    explicit TThrottlerWrapper(TThrottlerId throttlerName)
        : ThrottlerName_(std::move(throttlerName))
    { }

    void SetBinding(NConcurrency::IThroughputThrottlerPtr underlying, TDuration timeout)
    {
        Binding_.Store(TBinding{std::move(underlying), timeout});
    }

    TFuture<void> Throttle(i64 amount) override
    {
        if (amount < 0) {
            return MakeFuture(TError("Quota amount must be non-negative, got %v", amount));
        }
        auto binding = Binding_.Load();
        if (!binding.Underlying) {
            return MakeFuture(MakeDetachedError());
        }
        auto future = binding.Underlying->Throttle(amount);
        if (future.IsSet()) {
            return future;
        }
        future = future.WithTimeout(
            binding.Timeout,
            TFutureTimeoutOptions{
                .Error = TError("Error waiting for distributed throttler %Qv", ThrottlerName_),
            });
        // Keep the client that owns the pending request alive until it completes.
        future.Subscribe(BIND_NO_PROPAGATE([client = std::move(binding.Underlying)] (const TError&) {
        }));
        return future;
    }

    bool TryAcquire(i64 amount) override
    {
        return GetUnderlyingOrThrow()->TryAcquire(amount);
    }

    i64 TryAcquireAvailable(i64 amount) override
    {
        return GetUnderlyingOrThrow()->TryAcquireAvailable(amount);
    }

    void Acquire(i64 amount) override
    {
        GetUnderlyingOrThrow()->Acquire(amount);
    }

    void Release(i64 amount) override
    {
        GetUnderlyingOrThrow()->Release(amount);
    }

    bool IsOverdraft() override
    {
        return GetUnderlyingOrThrow()->IsOverdraft();
    }

    i64 GetQueueTotalAmount() const override
    {
        return GetUnderlyingOrThrow()->GetQueueTotalAmount();
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        return GetUnderlyingOrThrow()->GetEstimatedOverdraftDuration();
    }

    i64 GetAvailable() const override
    {
        return GetUnderlyingOrThrow()->GetAvailable();
    }

private:
    const TThrottlerId ThrottlerName_;
    TAtomicObject<TBinding> Binding_;

    NConcurrency::IThroughputThrottlerPtr GetUnderlyingOrThrow() const
    {
        auto underlying = Binding_.Load().Underlying;
        if (!underlying) {
            THROW_ERROR(MakeDetachedError());
        }
        return underlying;
    }

    TError MakeDetachedError() const
    {
        return TError("Throttler %Qv is not configured for this client", ThrottlerName_);
    }
};

using TThrottlerWrapperPtr = TIntrusivePtr<TThrottlerWrapper>;

////////////////////////////////////////////////////////////////////////////////

using TClientSpecs = THashMap<TThrottlerId, TDynamicThrottlerSpecPtr>;
using TClientSpecsPtr = std::shared_ptr<const TClientSpecs>;

class TJobThrottlerFactory
    : public IJobThrottlerFactory
{
public:
    TJobThrottlerFactory(
        NRpc::IChannelPtr controllerChannel,
        std::string clientId,
        TClientSpecsPtr specs,
        IStatusProfilerPtr statusProfiler,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler)
        : ControllerChannel_(std::move(controllerChannel))
        , ClientId_(std::move(clientId))
        , StatusProfiler_(std::move(statusProfiler))
        , Logger_(std::move(logger))
        , Profiler_(std::move(profiler))
        , Specs_(std::move(specs))
    {
    }

    NConcurrency::IThroughputThrottlerPtr GetClientOrThrow(std::string_view throttlerName) override
    {
        if (auto client = TryGetClient(throttlerName)) {
            return client;
        }
        THROW_ERROR_EXCEPTION("Throttler %Qv is not configured for this client",
            throttlerName);
    }

    NConcurrency::IThroughputThrottlerPtr TryGetClient(std::string_view throttlerName) override
    {
        auto guard = Guard(Lock_);
        if (auto it = Wrappers_.find(throttlerName); it != Wrappers_.end() && it->second.Spec) {
            return it->second.Wrapper;
        }
        auto it = Specs_->find(throttlerName);
        if (it == Specs_->end()) {
            return nullptr;
        }
        return EnsureWrapper(it->first, it->second, guard).Wrapper;
    }

    void SetPriority(TPriority priority) override
    {
        Priority_.store(priority, std::memory_order::relaxed);
    }

    void SetQuotaClasses(THashMap<TThrottlerId, TQuotaClassId> quotaClassIds) override
    {
        auto guard = Guard(Lock_);
        QuotaClassIds_ = std::move(quotaClassIds);
        for (auto& [name, entry] : Wrappers_) {
            entry.QuotaClassId->Store(GetQuotaClassId(name));
        }
    }

    void Reconfigure(TClientSpecsPtr specs)
    {
        auto guard = Guard(Lock_);
        Specs_ = std::move(specs);
        for (auto& [name, entry] : Wrappers_) {
            if (auto it = Specs_->find(name); it != Specs_->end()) {
                EnsureWrapper(name, it->second, guard);
            } else if (entry.Spec) {
                entry.Wrapper->SetBinding(nullptr, {});
                entry.Client.Reset();
                entry.Spec.Reset();
            }
        }
    }

private:
    using TQuotaClassIdHolderPtr = std::shared_ptr<TAtomicObject<TQuotaClassId>>;

    struct TWrapperEntry
    {
        TDynamicThrottlerSpecPtr Spec;
        TThrottlerWrapperPtr Wrapper;
        NConcurrency::IThroughputThrottlerPtr Client;
        //! Outlives client rebuilds, so a client built by an earlier
        //! Reconfigure keeps observing later class changes.
        TQuotaClassIdHolderPtr QuotaClassId;
    };

    const NRpc::IChannelPtr ControllerChannel_;
    const std::string ClientId_;
    const IStatusProfilerPtr StatusProfiler_;
    const NLogging::TLogger Logger_;
    const NProfiling::TProfiler Profiler_;

    TClientSpecsPtr Specs_;
    std::atomic<TPriority> Priority_{0};

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    THashMap<TThrottlerId, TWrapperEntry> Wrappers_;
    THashMap<TThrottlerId, TQuotaClassId> QuotaClassIds_;

    //! Caller must hold Lock_.
    TQuotaClassId GetQuotaClassId(const TThrottlerId& throttlerName) const
    {
        auto it = QuotaClassIds_.find(throttlerName);
        return it == QuotaClassIds_.end() ? TQuotaClassId{} : it->second;
    }

    // Make sure a wrapper for the given name exists and is wired to a client
    // built from |spec|. If the client-side part of the spec is equal to the
    // previous one, the existing client (and its prefetch state) is kept.
    // Caller must hold Lock_.
    TWrapperEntry& EnsureWrapper(
        const TThrottlerId& throttlerName,
        const TDynamicThrottlerSpecPtr& spec,
        const TGuard<TSpinLock>& /*guard*/)
    {
        auto& entry = Wrappers_[throttlerName];
        if (entry.Spec == spec) {
            return entry;
        }
        if (!entry.Wrapper) {
            entry.QuotaClassId = std::make_shared<TAtomicObject<TQuotaClassId>>(GetQuotaClassId(throttlerName));
            entry.Wrapper = New<TThrottlerWrapper>(throttlerName);
        }
        if (!entry.Spec || !entry.Spec->ClientConfigEquals(*spec)) {
            entry.Client = BuildClient(throttlerName, spec, entry.QuotaClassId);
        }
        entry.Wrapper->SetBinding(entry.Client, spec->ThrottleTimeout);
        entry.Spec = spec;
        return entry;
    }

    NConcurrency::IThroughputThrottlerPtr BuildClient(
        const TThrottlerId& throttlerName,
        const TDynamicThrottlerSpecPtr& spec,
        TQuotaClassIdHolderPtr quotaClassId)
    {
        if (spec->Mode == EThrottlerMode::Prefetching) {
            auto clientConfig = New<TDistributedThrottlerClientConfig>();
            clientConfig->ThrottlerName = throttlerName;
            clientConfig->ClientId = ClientId_;
            clientConfig->PrefetchingConfig = spec->BuildPrefetchingConfig();
            clientConfig->RetryingChannel = spec->RetryingChannel;
            clientConfig->RpcTimeout = spec->RpcTimeout;

            return CreateDistributedThrottler(
                std::move(clientConfig),
                ControllerChannel_,
                [weakSelf = MakeWeak(this)] () -> TPriority {
                    auto self = weakSelf.Lock();
                    return self ? self->Priority_.load(std::memory_order::relaxed) : TPriority{0};
                },
                [quotaClassId] () -> TQuotaClassId {
                    return quotaClassId->Load();
                },
                StatusProfiler_->WithPrefix(Format("/%v", throttlerName)),
                Logger_.WithTag("ThrottlerName", throttlerName),
                Profiler_
                    .WithTag("throttler_id", TString(throttlerName)));
        }
        THROW_ERROR_EXCEPTION("Unsupported throttler mode %Qlv", spec->Mode);
    }
};

class TWorkerThrottlerFactory
    : public IWorkerThrottlerFactory
{
public:
    TWorkerThrottlerFactory(
        const THashMap<NFlow::TThrottlerId, TDynamicThrottlerSpecPtr>& specs,
        NRpc::IChannelPtr controllerChannel)
        : ControllerChannel_(std::move(controllerChannel))
    {
        Reconfigure(specs);
    }

    IJobThrottlerFactoryPtr CreateFactory(
        std::string clientId,
        IStatusProfilerPtr statusProfiler,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler) override
    {
        std::lock_guard guard(Mutex_);
        auto factory = New<TJobThrottlerFactory>(
            ControllerChannel_,
            std::move(clientId),
            Specs_,
            std::move(statusProfiler),
            std::move(logger),
            std::move(profiler));
        if (std::ssize(Factories_) >= NextCleanupSize_) {
            std::erase_if(Factories_, [] (const auto& weakFactory) {
                return weakFactory.IsExpired();
            });
            NextCleanupSize_ = std::max<ssize_t>(64, 2 * std::ssize(Factories_));
        }
        Factories_.push_back(MakeWeak(factory));
        return factory;
    }

    void Reconfigure(const THashMap<NFlow::TThrottlerId, TDynamicThrottlerSpecPtr>& specs) override
    {
        auto clients = std::make_shared<TClientSpecs>();
        for (const auto& [id, spec] : specs) {
            clients->emplace(id.Underlying(), spec);
        }
        std::lock_guard guard(Mutex_);
        Specs_ = std::move(clients);
        std::erase_if(Factories_, [&] (const auto& weakFactory) {
            if (auto factory = weakFactory.Lock()) {
                factory->Reconfigure(Specs_);
                return false;
            }
            return true;
        });
    }

private:
    const NRpc::IChannelPtr ControllerChannel_;

    std::mutex Mutex_;
    TClientSpecsPtr Specs_;
    ssize_t NextCleanupSize_ = 64;
    std::vector<TWeakPtr<TJobThrottlerFactory>> Factories_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IWorkerThrottlerFactoryPtr CreateWorkerThrottlerFactory(
    const THashMap<NFlow::TThrottlerId, TDynamicThrottlerSpecPtr>& specs,
    NRpc::IChannelPtr controllerChannel)
{
    return New<TWorkerThrottlerFactory>(specs, std::move(controllerChannel));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
