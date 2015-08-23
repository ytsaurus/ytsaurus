#include "stdafx.h"
#include "response_keeper.h"
#include "config.h"
#include "helpers.h"
#include "service.h"
#include "private.h"

#include <core/concurrency/periodic_executor.h>

#include <core/actions/invoker_util.h>

#include <core/profiling/profiler.h>
#include <core/profiling/timing.h>

namespace NYT {
namespace NRpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto EvictionPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TResponseKeeper::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TResponseKeeperConfigPtr config,
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler)
        : Config_(std::move(config))
        , Logger(logger)
        , Profiler(profiler)
        , CountCounter_("/kept_response_count")
        , SpaceCounter_("/kept_response_space")
    {
        YCHECK(Config_);

        EvictionExecutor_ = New<TPeriodicExecutor>(
            GetSyncInvoker(),
            BIND(&TImpl::OnEvict, MakeWeak(this)),
            EvictionPeriod);
        EvictionExecutor_->Start();
    }

    void Start()
    {
        {
            if (Started_)
                return;

            TGuard<TSpinLock> guard(SpinLock_);
            WarmupDeadline_ = Config_->EnableWarmup
                ? NProfiling::GetCpuInstant() + NProfiling::DurationToCpuDuration(Config_->WarmupTime)
                : 0;
            Started_ = true;
        }

        LOG_INFO("Response keeper started (WarmupTime: %v, ExpirationTime: %v)",
            Config_->WarmupTime,
            Config_->ExpirationTime);
    }

    void Stop()
    {
        {
            if (!Started_)
                return;

            TGuard<TSpinLock> guard(SpinLock_);
            PendingResponses_.clear();
            FinishedResponses_.clear();
            ResponseEvictionQueue_.clear();
            FinishedResponseSpace_ = 0;
            FinishedResponseCount_ = 0;
            Started_ = false;
        }

        OnProfiling();

        LOG_INFO("Response keeper stopped");
    }

    TFuture<TSharedRefArray> TryBeginRequest(const TMutationId& id, bool isRetry)
    {
        YASSERT(id);

        TGuard<TSpinLock> guard(SpinLock_);

        if (!Started_) {
            THROW_ERROR_EXCEPTION("Response keeper is not active");
        }

        auto pendingIt = PendingResponses_.find(id);
        if (pendingIt != PendingResponses_.end()) {
            if (!isRetry) {
                THROW_ERROR_EXCEPTION("Duplicate request is not marked as \"retry\"")
                    << TErrorAttribute("mutation_id", id);
            }
            LOG_DEBUG("Replying with pending response (MutationId: %v)", id);
            return pendingIt->second;
        }

        auto finishedIt = FinishedResponses_.find(id);
        if (finishedIt != FinishedResponses_.end()) {
            if (!isRetry) {
                THROW_ERROR_EXCEPTION("Duplicate request is not marked as \"retry\"")
                    << TErrorAttribute("mutation_id", id);
            }
            LOG_DEBUG("Replying with finished response (MutationId: %v)", id);
            return MakeFuture(finishedIt->second);
        }

        if (isRetry && NProfiling::GetCpuInstant() < WarmupDeadline_) {
            THROW_ERROR_EXCEPTION("Cannot reliably check for a duplicate mutating request")
                << TErrorAttribute("mutation_id", id)
                << TErrorAttribute("warmup_time", Config_->WarmupTime);
        }

        YCHECK(PendingResponses_.insert(std::make_pair(id, NewPromise<TSharedRefArray>())).second);

        LOG_TRACE("Response will be kept (MutationId: %v)", id);

        return TFuture<TSharedRefArray>();
    }

    void EndRequest(const TMutationId& id, TSharedRefArray response)
    {
        YASSERT(id);

        TPromise<TSharedRefArray> promise;
        {
            TGuard<TSpinLock> guard(SpinLock_);

            if (!Started_)
                return;

            auto pendingIt = PendingResponses_.find(id);
            if (pendingIt != PendingResponses_.end()) {
                promise = pendingIt->second;
                PendingResponses_.erase(pendingIt);
            }

            // NB: Allow duplicates.
            if (!FinishedResponses_.insert(std::make_pair(id, response)).second)
                return;

            ResponseEvictionQueue_.push_back(TEvictionItem{id, NProfiling::GetCpuInstant()});
        }

        if (promise) {
            promise.Set(response);
        }

        LOG_TRACE("Response kept (MutationId: %v)", id);
        UpdateCounters(response, +1);
        OnProfiling();
    }

    void CancelRequest(const TMutationId& id)
    {
        YASSERT(id);

        {
            TGuard<TSpinLock> guard(SpinLock_);

            if (!Started_)
                return;

            PendingResponses_.erase(id);
        }
    }

    bool TryReplyFrom(IServiceContextPtr context)
    {
        auto mutationId = GetMutationId(context);
        if (!mutationId) {
            return false;
        }

        auto keptAsyncResponseMessage = TryBeginRequest(mutationId, context->IsRetry());
        if (keptAsyncResponseMessage) {
            context->ReplyFrom(std::move(keptAsyncResponseMessage));
            return true;
        } else {
            context->GetAsyncResponseMessage().Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TSharedRefArray>&) {
                if (context->GetError().GetCode() == NRpc::EErrorCode::Unavailable) {
                    CancelRequest(mutationId);
                } else {
                    EndRequest(mutationId, context->GetResponseMessage());
                }
            }));
            return false;
        }
    }

private:
    const TResponseKeeperConfigPtr Config_;

    TPeriodicExecutorPtr EvictionExecutor_;

    bool Started_ = false;
    NProfiling::TCpuInstant WarmupDeadline_ = 0;

    yhash_map<TMutationId, TSharedRefArray> FinishedResponses_;
    volatile int FinishedResponseCount_ = 0;
    volatile i64 FinishedResponseSpace_ = 0;

    struct TEvictionItem
    {
        TMutationId Id;
        NProfiling::TCpuInstant When;
    };

    std::deque<TEvictionItem> ResponseEvictionQueue_;

    yhash_map<TMutationId, TPromise<TSharedRefArray>> PendingResponses_;

    NLogging::TLogger Logger;

    NProfiling::TProfiler Profiler;
    NProfiling::TAggregateCounter CountCounter_;
    NProfiling::TAggregateCounter SpaceCounter_;

    TSpinLock SpinLock_;


    void UpdateCounters(const TSharedRefArray& data, int delta)
    {
        FinishedResponseCount_ += delta;

        for (const auto& part : data) {
            FinishedResponseSpace_ += delta * part.Size();
        }
    }

    void OnProfiling()
    {
        Profiler.Update(CountCounter_, FinishedResponseCount_);
        Profiler.Update(SpaceCounter_, FinishedResponseSpace_);
    }

    void OnEvict()
    {
        bool changed = false;

        {
            TGuard<TSpinLock> guard(SpinLock_);

            if (!Started_)
                return;

            auto deadline = NProfiling::GetCpuInstant() - NProfiling::DurationToCpuDuration(Config_->ExpirationTime);
            while (!ResponseEvictionQueue_.empty()) {
                const auto& item = ResponseEvictionQueue_.front();
                if (item.When > deadline) {
                    break;
                }

                auto it = FinishedResponses_.find(item.Id);
                YCHECK(it != FinishedResponses_.end());
                UpdateCounters(it->second, -1);
                FinishedResponses_.erase(it);
                ResponseEvictionQueue_.pop_front();
                changed = true;
            }
        }

        if (changed) {
            OnProfiling();
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TResponseKeeper::TResponseKeeper(
    TResponseKeeperConfigPtr config,
    const NLogging::TLogger& logger,
    const NProfiling::TProfiler& profiler)
    : Impl_(New<TImpl>(std::move(config), logger, profiler))
{ }

TResponseKeeper::~TResponseKeeper()
{ }

void TResponseKeeper::Start()
{
    Impl_->Start();
}

void TResponseKeeper::Stop()
{
    Impl_->Stop();
}

TFuture<TSharedRefArray> TResponseKeeper::TryBeginRequest(const TMutationId& id, bool isRetry)
{
    return Impl_->TryBeginRequest(id, isRetry);
}

void TResponseKeeper::EndRequest(const TMutationId& id, TSharedRefArray response)
{
    Impl_->EndRequest(id, std::move(response));
}

void TResponseKeeper::CancelRequest(const TMutationId& id)
{
    Impl_->CancelRequest(id);
}

bool TResponseKeeper::TryReplyFrom(IServiceContextPtr context)
{
    return Impl_->TryReplyFrom(std::move(context));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
