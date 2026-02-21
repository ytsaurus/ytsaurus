#include "persistent_response_keeper.h"

#include "config.h"
#include "serialize.h"
#include "mutation_context.h"
#include "private.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/rpc/response_keeper.h>
#include <yt/yt/core/rpc/service.h>

#include <yt/yt/core/misc/ring_queue.h>
#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

namespace NYT::NHydra {

using namespace NThreading;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

struct TEvictionItem
{
    TMutationId Id;
    TInstant When;

    // For Load.
    TEvictionItem() = default;
    TEvictionItem(TMutationId id, TInstant when)
        : Id(id)
        , When(when)
    { }

    void Save(TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, Id);
        Save(context, When);
    }

    void Load(TLoadContext& context)
    {
        using NYT::Load;

        Load(context, Id);
        Load(context, When);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPersistentResponseKeeper
    : public IPersistentResponseKeeper
{
public:
    TPersistentResponseKeeper(
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler)
        : Logger(logger)
    {
        profiler.AddFuncGauge("/response_keeper/kept_response_count", MakeStrong(this), [this] {
            return FinishedResponseCount_.load(std::memory_order::relaxed);
        });
        profiler.AddFuncGauge("/response_keeper/kept_response_space", MakeStrong(this), [this] {
            return FinishedResponseSpace_.load(std::memory_order::relaxed);
        });
        profiler.AddFuncGauge("/response_keeper/pending_response_count", MakeStrong(this), [this] {
            return PendingResponseCount_.load(std::memory_order::relaxed);
        });
    }

    void Start() override
    { }

    void Stop() override
    { }

    TFuture<TSharedRefArray> TryBeginRequest(TMutationId id, bool isRetry) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = WriterGuard(Lock_);

        return DoTryBeginRequest(id, isRetry);
    }

    TFuture<TSharedRefArray> FindRequest(TMutationId id, bool isRetry) const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(Lock_);

        return DoFindRequest(id, isRetry);
    }

    std::function<void()> EndRequest(
        TMutationId id,
        TSharedRefArray response,
        bool remember) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_ASSERT(id);

        if (!response) {
            YT_LOG_ALERT("Null response passed to response keeper (MutationId: %v, Remember: %v)",
                id,
                remember);
        }

        auto space = static_cast<i64>(GetByteSize(response));

        auto guard = WriterGuard(Lock_);

        // Persistent part.
        if (remember) {
            YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
            YT_VERIFY(HasMutationContext());

            // NB: Allow duplicates.
            auto [it, inserted] = FinishedResponses_.emplace(id, response);

            if (inserted) {
                ui64 responseHash = 0;
                for (const auto& part : response) {
                    responseHash = CombineHashes(responseHash, FarmFingerprint(part.begin(), part.size()));
                }

                auto* mutationContext = GetCurrentMutationContext();
                mutationContext->CombineStateHash(responseHash);

                ResponseEvictionQueue_.emplace_back(id, mutationContext->GetTimestamp());

                FinishedResponseCount_.fetch_add(1, std::memory_order::relaxed);
                FinishedResponseSpace_.fetch_add(space, std::memory_order::relaxed);

                YT_LOG_DEBUG("Response added to persistent response keeper "
                    "(MutationId: %v, ResponseHash: %v)",
                    id,
                    responseHash);
            }
        }

        // Transient part.
        auto promise = TakePendingResponse(id);
        guard.Release();

        if (promise) {
            return [promise = std::move(promise), response = std::move(response)] () mutable {
                promise.TrySet(std::move(response));
            };
        } else {
            return {};
        }
    }

    std::function<void()> EndRequest(
        TMutationId id,
        TErrorOr<TSharedRefArray> responseOrError,
        bool remember) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        YT_ASSERT(id);

        if (responseOrError.IsOK()) {
            return EndRequest(id, std::move(responseOrError).Value(), remember);
        }

        auto guard = WriterGuard(Lock_);

        auto promise = TakePendingResponse(id);
        guard.Release();

        if (promise) {
            return [promise = std::move(promise), responseOrError = std::move(responseOrError)] () mutable {
                promise.TrySet(std::move(responseOrError));
            };
        } else {
            return {};
        }
    }

    void CancelPendingRequests(const TError& error) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = WriterGuard(Lock_);

        auto pendingResponses = std::move(PendingResponses_);
        PendingResponseCount_.store(0, std::memory_order::relaxed);

        guard.Release();

        for (const auto& [id, promise] : pendingResponses) {
            promise.TrySet(error);
        }

        YT_LOG_INFO(error, "All pending requests canceled");
    }

    void Clear() override
    {
        FinishedResponses_.clear();
        FinishedResponseCount_.store(0, std::memory_order::relaxed);
        FinishedResponseSpace_.store(0, std::memory_order::relaxed);
        ResponseEvictionQueue_.clear();
    }

    void Save(TSaveContext& context) const override
    {
        using NYT::Save;

        Save(context, FinishedResponses_);
        Save(context, FinishedResponseCount_.load(std::memory_order::acquire));
        Save(context, FinishedResponseSpace_.load(std::memory_order::acquire));
        Save(context, ResponseEvictionQueue_);
    }

    void Load(TLoadContext& context) override
    {
        using NYT::Load;

        Load(context, FinishedResponses_);
        FinishedResponseCount_.store(
            Load<decltype(FinishedResponseCount_)::value_type>(context),
            std::memory_order::relaxed);
        FinishedResponseSpace_.store(
            Load<decltype(FinishedResponseSpace_)::value_type>(context),
            std::memory_order::relaxed);
        Load(context, ResponseEvictionQueue_);
    }

    void Evict(TDuration expirationTimeout, int maxResponseCountPerEvictionPass, i64 maxResponsesSpace) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto guard = WriterGuard(Lock_);

        YT_LOG_DEBUG("Response keeper eviction pass started (ExpirationTimeout: %v, MaxResponseCountPerEvictionPass: %v)",
            expirationTimeout,
            maxResponseCountPerEvictionPass);

        int counter = 0;
        auto deadline = GetCurrentMutationContext()->GetTimestamp() - expirationTimeout;
        while (!ResponseEvictionQueue_.empty()) {
            const auto& item = ResponseEvictionQueue_.front();
            if (item.When > deadline &&
                FinishedResponseSpace_.load(std::memory_order::acquire) <= maxResponsesSpace) {
                break;
            }

            if (counter > maxResponseCountPerEvictionPass) {
                YT_LOG_WARNING("Response keeper eviction pass interrupted (ResponseCount: %v, ResponsesLeft: %v, OccupiedSpace: %v)",
                    counter,
                    FinishedResponseCount_.load(std::memory_order::relaxed),
                    FinishedResponseSpace_.load(std::memory_order::relaxed));
                break;
            }

            auto it = FinishedResponses_.find(item.Id);
            YT_VERIFY(it != FinishedResponses_.end());

            ++counter;
            FinishedResponseCount_.fetch_sub(1, std::memory_order::relaxed);
            FinishedResponseSpace_.fetch_sub(
                static_cast<i64>(GetByteSize(it->second)), std::memory_order::relaxed);

            FinishedResponses_.erase(it);
            ResponseEvictionQueue_.pop_front();
        }

        YT_LOG_DEBUG("Response keeper eviction pass completed (ResponseCount: %v)",
            counter);
    }

private:
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, Lock_);

    THashMap<TMutationId, TPromise<TSharedRefArray>> PendingResponses_;
    // For profiling only.
    std::atomic<i64> PendingResponseCount_ = 0;

    // Persistent.
    using TFinishedResponseMap = THashMap<TMutationId, TSharedRefArray>;
    TFinishedResponseMap FinishedResponses_;
    // For profiling only. Saved to snapshot to avoid recomputing after loading.
    std::atomic<int> FinishedResponseCount_ = 0;
    // For limiting memory footprint and profiling. Saved to snapshot to avoid recomputing after loading.
    std::atomic<i64> FinishedResponseSpace_ = 0;
    // Serializable queue.
    std::deque<TEvictionItem> ResponseEvictionQueue_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    TPromise<TSharedRefArray> TakePendingResponse(TMutationId id)
    {
        auto pendingIt = PendingResponses_.find(id);
        if (pendingIt == PendingResponses_.end()) {
            return {};
        }

        auto promise = std::move(pendingIt->second);
        PendingResponses_.erase(pendingIt);
        PendingResponseCount_.fetch_sub(1, std::memory_order::relaxed);

        return promise;
    }

    TFuture<TSharedRefArray> DoTryBeginRequest(TMutationId id, bool isRetry)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        YT_VERIFY(!HasMutationContext());
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        auto result = DoFindRequest(id, isRetry);
        if (!result) {
            EmplaceOrCrash(PendingResponses_, std::pair(id, NewPromise<TSharedRefArray>()));
            PendingResponseCount_.fetch_add(1, std::memory_order::relaxed);
        }
        return result;
    }

    TFuture<TSharedRefArray> DoFindRequest(TMutationId id, bool isRetry) const
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        YT_VERIFY(!HasMutationContext());
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        YT_ASSERT(id);

        auto pendingIt = PendingResponses_.find(id);
        if (pendingIt != PendingResponses_.end()) {
            ValidateRetry(id, isRetry);

            YT_LOG_DEBUG("Replying with pending response (MutationId: %v)", id);
            return pendingIt->second;
        }

        auto finishedIt = FinishedResponses_.find(id);
        if (finishedIt != FinishedResponses_.end()) {
            ValidateRetry(id, isRetry);

            YT_LOG_DEBUG("Replying with finished response (MutationId: %v)", id);
            return MakeFuture(finishedIt->second);
        }
        return {};
    }

    bool IsWarmingUp() const override
    {
        return false;
    }

    bool TryReplyFrom(const IServiceContextPtr& context, bool subscribeToResponse) override
    {
        // If anyone ever needs this as true, you have to schedule mutation or something like that.
        YT_VERIFY(!subscribeToResponse);

        YT_ASSERT_THREAD_AFFINITY_ANY();
        YT_VERIFY(!HasMutationContext());

        auto guard = WriterGuard(Lock_);

        auto mutationId = context->GetMutationId();
        if (!mutationId) {
            return false;
        }

        if (auto keptAsyncResponseMessage = DoTryBeginRequest(mutationId, context->IsRetry())) {
            context->ReplyFrom(std::move(keptAsyncResponseMessage));
            return true;
        }

        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

IPersistentResponseKeeperPtr CreatePersistentResponseKeeper(
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
{
    return New<TPersistentResponseKeeper>(
        logger,
        profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
