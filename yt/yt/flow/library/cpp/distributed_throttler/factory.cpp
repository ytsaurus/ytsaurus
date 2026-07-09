#include "factory.h"

#include "client.h"

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

namespace {

// Stable wrapper around an IThroughputThrottler whose underlying instance can
// be swapped atomically. Returned to users from GetClient and reused across
// Reconfigure: when the spec changes the factory rebuilds the inner client
// and stores it here, so a cached pointer keeps working with the fresh config.
class TThrottlerWrapper
    : public NConcurrency::IThroughputThrottler
{
public:
    explicit TThrottlerWrapper(TThrottlerId throttlerName)
        : ThrottlerName_(std::move(throttlerName))
    { }

    void SetUnderlying(NConcurrency::IThroughputThrottlerPtr underlying)
    {
        Underlying_.Store(std::move(underlying));
    }

    TFuture<void> Throttle(i64 amount) override
    {
        if (auto underlying = Underlying_.Acquire()) {
            return underlying->Throttle(amount);
        }
        return MakeFuture(MakeDetachedError());
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
    TAtomicIntrusivePtr<NConcurrency::IThroughputThrottler> Underlying_;

    NConcurrency::IThroughputThrottlerPtr GetUnderlyingOrThrow() const
    {
        auto underlying = Underlying_.Acquire();
        if (!underlying) {
            THROW_ERROR(MakeDetachedError());
        }
        return underlying;
    }

    TError MakeDetachedError() const
    {
        return TError("Throttler %Qv is not configured for this client",
            ThrottlerName_);
    }
};

using TThrottlerWrapperPtr = TIntrusivePtr<TThrottlerWrapper>;

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerFactory
    : public IDistributedThrottlerFactory
{
public:
    TDistributedThrottlerFactory(
        std::function<NRpc::IChannelPtr()> channelProvider,
        std::string clientId,
        THashMap<TThrottlerId, TDynamicThrottlerSpecPtr> initialThrottlers,
        IStatusProfilerPtr statusProfiler,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler)
        : ChannelProvider_(std::move(channelProvider))
        , ClientId_(std::move(clientId))
        , StatusProfiler_(std::move(statusProfiler))
        , Logger_(std::move(logger))
        , Profiler_(std::move(profiler))
    {
        auto guard = Guard(Lock_);
        for (const auto& [name, spec] : initialThrottlers) {
            EnsureWrapper(name, spec, guard);
        }
    }

    NConcurrency::IThroughputThrottlerPtr GetClient(std::string_view throttlerName) override
    {
        auto guard = Guard(Lock_);
        auto it = Wrappers_.find(throttlerName);
        if (it == Wrappers_.end() || !it->second.Spec) {
            THROW_ERROR_EXCEPTION("Throttler %Qv is not configured for this client",
                throttlerName);
        }
        return it->second.Wrapper;
    }

    void SetPriority(TPriority priority) override
    {
        Priority_.store(priority, std::memory_order::relaxed);
    }

    void Reconfigure(
        THashMap<TThrottlerId, TDynamicThrottlerSpecPtr> throttlers) override
    {
        auto guard = Guard(Lock_);

        // Detach wrappers whose name is gone from the new config. The wrapper
        // itself stays in the map so a previously returned pointer stays
        // stable; subsequent calls on it raise "not configured".
        for (auto& [name, entry] : Wrappers_) {
            if (!throttlers.contains(name)) {
                entry.Spec.Reset();
                entry.Wrapper->SetUnderlying(nullptr);
            }
        }

        for (const auto& [name, spec] : throttlers) {
            EnsureWrapper(name, spec, guard);
        }
    }

private:
    struct TWrapperEntry
    {
        TDynamicThrottlerSpecPtr Spec;
        TThrottlerWrapperPtr Wrapper;
    };

    const std::function<NRpc::IChannelPtr()> ChannelProvider_;
    const std::string ClientId_;
    const IStatusProfilerPtr StatusProfiler_;
    const NLogging::TLogger Logger_;
    const NProfiling::TProfiler Profiler_;

    std::atomic<TPriority> Priority_{0};

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TThrottlerId, TWrapperEntry> Wrappers_;

    // Make sure a wrapper for the given name exists and is wired to a client
    // built from |spec|. If the spec is byte-equal to the previous one, the
    // existing client (and its prefetch state) is kept. Caller must hold Lock_.
    void EnsureWrapper(
        const TThrottlerId& throttlerName,
        const TDynamicThrottlerSpecPtr& spec,
        const TGuard<NThreading::TSpinLock>& /*guard*/)
    {
        auto& entry = Wrappers_[throttlerName];
        if (!entry.Wrapper) {
            entry.Wrapper = New<TThrottlerWrapper>(throttlerName);
        } else if (entry.Spec && *entry.Spec == *spec) {
            // Refresh the pointer so the previous shared spec can be released.
            entry.Spec = spec;
            return;
        }
        entry.Spec = spec;
        entry.Wrapper->SetUnderlying(BuildClient(throttlerName, spec));
    }

    NConcurrency::IThroughputThrottlerPtr BuildClient(
        const TThrottlerId& throttlerName,
        const TDynamicThrottlerSpecPtr& spec)
    {
        auto clientConfig = New<TDistributedThrottlerClientConfig>();
        clientConfig->ThrottlerName = throttlerName;
        clientConfig->ClientId = ClientId_;
        clientConfig->PrefetchingConfig = spec->BuildPrefetchingConfig();
        clientConfig->RetryingChannel = spec->RetryingChannel;
        clientConfig->RpcTimeout = spec->RpcTimeout;

        return CreateDistributedThrottler(
            std::move(clientConfig),
            ChannelProvider_,
            [weakSelf = MakeWeak(this)] () -> TPriority {
                auto self = weakSelf.Lock();
                return self ? self->Priority_.load(std::memory_order::relaxed) : TPriority{0};
            },
            StatusProfiler_->WithPrefix(Format("/%v", throttlerName)),
            Logger_.WithTag("ThrottlerName: %v", throttlerName),
            Profiler_
                .WithTag("throttler_id", TString(throttlerName)));
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IDistributedThrottlerFactoryPtr CreateDistributedThrottlerFactory(
    std::function<NRpc::IChannelPtr()> channelProvider,
    std::string clientId,
    THashMap<TThrottlerId, TDynamicThrottlerSpecPtr> initialThrottlers,
    IStatusProfilerPtr statusProfiler,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler)
{
    return New<TDistributedThrottlerFactory>(
        std::move(channelProvider),
        std::move(clientId),
        std::move(initialThrottlers),
        std::move(statusProfiler),
        std::move(logger),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
