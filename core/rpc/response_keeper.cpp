#include "response_keeper.h"
#include "private.h"
#include "config.h"
#include "helpers.h"
#include "service.h"

#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/timing.h>

namespace NYT::NRpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto EvictionPeriod = TDuration::Seconds(1);
static const auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TResponseKeeper::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TResponseKeeperConfigPtr config,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler)
        : Config_(std::move(config))
        , Invoker_(std::move(invoker))
        , Logger(logger)
        , Profiler(profiler)
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Invoker_);
        VERIFY_INVOKER_THREAD_AFFINITY(Invoker_, HomeThread);

        EvictionExecutor_ = New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TImpl::OnEvict, MakeWeak(this)),
            EvictionPeriod);
        EvictionExecutor_->Start();

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor_->Start();
    }

    void Start()
    {
        VERIFY_THREAD_AFFINITY(HomeThread);

        if (Started_) {
            return;
        }

        WarmupDeadline_ = Config_->EnableWarmup
            ? NProfiling::GetCpuInstant() + NProfiling::DurationToCpuDuration(Config_->WarmupTime)
            : 0;
        Started_ = true;

        YT_LOG_INFO("Response keeper started (WarmupTime: %v, ExpirationTime: %v)",
            Config_->WarmupTime,
            Config_->ExpirationTime);
    }

    void Stop()
    {
        VERIFY_THREAD_AFFINITY(HomeThread);

        if (!Started_) {
            return;
        }

        PendingResponses_.clear();
        FinishedResponses_.clear();
        ResponseEvictionQueue_.clear();
        FinishedResponseSpace_ = 0;
        FinishedResponseCount_ = 0;
        Started_ = false;

        YT_LOG_INFO("Response keeper stopped");
    }

    TFuture<TSharedRefArray> TryBeginRequest(TMutationId id, bool isRetry)
    {
        VERIFY_THREAD_AFFINITY(HomeThread);
        YT_ASSERT(id);

        if (!Started_) {
            THROW_ERROR_EXCEPTION("Response keeper is not active");
        }

        auto pendingIt = PendingResponses_.find(id);
        if (pendingIt != PendingResponses_.end()) {
            if (!isRetry) {
                THROW_ERROR_EXCEPTION("Duplicate request is not marked as \"retry\"")
                    << TErrorAttribute("mutation_id", id);
            }
            YT_LOG_DEBUG("Replying with pending response (MutationId: %v)", id);
            return pendingIt->second;
        }

        auto finishedIt = FinishedResponses_.find(id);
        if (finishedIt != FinishedResponses_.end()) {
            if (!isRetry) {
                THROW_ERROR_EXCEPTION("Duplicate request is not marked as \"retry\"")
                    << TErrorAttribute("mutation_id", id);
            }
            YT_LOG_DEBUG("Replying with finished response (MutationId: %v)", id);
            return MakeFuture(finishedIt->second);
        }

        if (isRetry && IsWarmingUp()) {
            THROW_ERROR_EXCEPTION("Cannot reliably check for a duplicate mutating request")
                << TErrorAttribute("mutation_id", id)
                << TErrorAttribute("warmup_time", Config_->WarmupTime);
        }

        YT_VERIFY(PendingResponses_.insert(std::make_pair(id, NewPromise<TSharedRefArray>())).second);

        return TFuture<TSharedRefArray>();
    }

    void EndRequest(TMutationId id, TSharedRefArray response, bool remember)
    {
        VERIFY_THREAD_AFFINITY(HomeThread);
        YT_ASSERT(id);

        if (!Started_) {
            return;
        }

        auto pendingIt = PendingResponses_.find(id);

        TPromise<TSharedRefArray> promise;
        if (pendingIt != PendingResponses_.end()) {
            promise = std::move(pendingIt->second);
            PendingResponses_.erase(pendingIt);
        }

        if (remember) {
            // NB: Allow duplicates.
            if (!FinishedResponses_.insert(std::make_pair(id, response)).second) {
                return;
            }

            ResponseEvictionQueue_.push_back(TEvictionItem{id, NProfiling::GetCpuInstant()});

            UpdateCounters(response, +1);
        }

        if (promise) {
            promise.Set(response);
        }
    }

    void EndRequest(TMutationId id, TErrorOr<TSharedRefArray> responseOrError, bool remember)
    {
        VERIFY_THREAD_AFFINITY(HomeThread);
        YT_ASSERT(id);

        if (!Started_) {
            return;
        }

        if (responseOrError.IsOK()) {
            EndRequest(id, std::move(responseOrError.Value()), remember);
            return;
        }

        auto it = PendingResponses_.find(id);
        if (it == PendingResponses_.end()) {
            return;
        }

        auto promise = std::move(it->second);
        PendingResponses_.erase(it);

        promise.Set(TError(responseOrError));
    }

    void CancelPendingRequests(const TError& error)
    {
        VERIFY_THREAD_AFFINITY(HomeThread);

        if (!Started_) {
            return;
        }

        auto pendingResponses = std::move(PendingResponses_);
        for (auto& [id, promise] : pendingResponses) {
            promise.Set(error);
        }

        YT_LOG_INFO(error, "All pending requests canceled");
    }

    bool TryReplyFrom(const IServiceContextPtr& context)
    {
        VERIFY_THREAD_AFFINITY(HomeThread);

        auto mutationId = context->GetMutationId();
        if (!mutationId) {
            return false;
        }

        auto keptAsyncResponseMessage = TryBeginRequest(mutationId, context->IsRetry());
        if (keptAsyncResponseMessage) {
            context->ReplyFrom(std::move(keptAsyncResponseMessage));
            return true;
        } else {
            context->GetAsyncResponseMessage()
                .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TSharedRefArray>&) {
                    const auto& error = context->GetError();
                    EndRequest(
                        mutationId,
                        context->GetResponseMessage(),
                        error.GetCode() != NRpc::EErrorCode::Unavailable);
                }).Via(Invoker_));
            return false;
        }
    }

    bool IsWarmingUp() const
    {
        return NProfiling::GetCpuInstant() < WarmupDeadline_;
    }

private:
    const TResponseKeeperConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler;

    TPeriodicExecutorPtr EvictionExecutor_;
    TPeriodicExecutorPtr ProfilingExecutor_;

    bool Started_ = false;
    NProfiling::TCpuInstant WarmupDeadline_ = 0;

    THashMap<TMutationId, TSharedRefArray> FinishedResponses_;
    int FinishedResponseCount_ = 0;
    i64 FinishedResponseSpace_ = 0;

    struct TEvictionItem
    {
        TMutationId Id;
        NProfiling::TCpuInstant When;
    };

    std::deque<TEvictionItem> ResponseEvictionQueue_;

    THashMap<TMutationId, TPromise<TSharedRefArray>> PendingResponses_;

    NProfiling::TAggregateGauge CountCounter_{"/kept_response_count"};
    NProfiling::TAggregateGauge SpaceCounter_{"/kept_response_space"};

    DECLARE_THREAD_AFFINITY_SLOT(HomeThread);


    void UpdateCounters(const TSharedRefArray& data, int delta)
    {
        FinishedResponseCount_ += delta;
        for (const auto& part : data) {
            FinishedResponseSpace_ += delta * part.Size();
        }
    }

    void OnProfiling()
    {
        VERIFY_THREAD_AFFINITY(HomeThread);

        if (!Started_) {
            return;
        }

        Profiler.Update(CountCounter_, FinishedResponseCount_);
        Profiler.Update(SpaceCounter_, FinishedResponseSpace_);
    }

    void OnEvict()
    {
        VERIFY_THREAD_AFFINITY(HomeThread);

        if (!Started_) {
            return;
        }

        auto deadline = NProfiling::GetCpuInstant() - NProfiling::DurationToCpuDuration(Config_->ExpirationTime);
        while (!ResponseEvictionQueue_.empty()) {
            const auto& item = ResponseEvictionQueue_.front();
            if (item.When > deadline) {
                break;
            }

            auto it = FinishedResponses_.find(item.Id);
            YT_VERIFY(it != FinishedResponses_.end());
            UpdateCounters(it->second, -1);
            FinishedResponses_.erase(it);
            ResponseEvictionQueue_.pop_front();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TResponseKeeper::TResponseKeeper(
    TResponseKeeperConfigPtr config,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
    : Impl_(New<TImpl>(
        std::move(config),
        std::move(invoker),
        logger,
        profiler))
{ }

TResponseKeeper::~TResponseKeeper() = default;

void TResponseKeeper::Start()
{
    Impl_->Start();
}

void TResponseKeeper::Stop()
{
    Impl_->Stop();
}

TFuture<TSharedRefArray> TResponseKeeper::TryBeginRequest(TMutationId id, bool isRetry)
{
    return Impl_->TryBeginRequest(id, isRetry);
}

void TResponseKeeper::EndRequest(TMutationId id, TSharedRefArray response, bool remember)
{
    Impl_->EndRequest(id, std::move(response), remember);
}

void TResponseKeeper::EndRequest(TMutationId id, TErrorOr<TSharedRefArray> responseOrError, bool remember)
{
    Impl_->EndRequest(id, std::move(responseOrError), remember);
}

void TResponseKeeper::CancelPendingRequests(const TError& error)
{
    Impl_->CancelPendingRequests(error);
}

bool TResponseKeeper::TryReplyFrom(const IServiceContextPtr& context)
{
    return Impl_->TryReplyFrom(context);
}

bool TResponseKeeper::IsWarmingUp() const
{
    return Impl_->IsWarmingUp();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
