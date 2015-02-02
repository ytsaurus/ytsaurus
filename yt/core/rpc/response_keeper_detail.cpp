#include "stdafx.h"
#include "response_keeper_detail.h"
#include "config.h"
#include "private.h"

#include <core/misc/serialize.h>

namespace NYT {
namespace NRpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const auto& Logger = RpcServerLogger;

////////////////////////////////////////////////////////////////////////////////

TResponseKeeperBase::TResponseKeeperBase(
    TResponseKeeperConfigPtr config,
    const NProfiling::TProfiler& profiler)
    : Config_(std::move(config))
    , Profiler(profiler)
    , CountCounter_("/kept_response_count")
    , SpaceCounter_("/kept_response_space")
{
    YCHECK(Config_);
}

TFuture<TSharedRefArray> TResponseKeeperBase::TryBeginRequest(const TMutationId& id)
{
    YASSERT(id != NullMutationId);

    TGuard<TSpinLock> guard(SpinLock_);

    auto finishedIt = FinishedResponses_.find(id);
    if (finishedIt != FinishedResponses_.end()) {
        LOG_DEBUG("Replying with finished response (MutationId: %v)", id);
        return MakeFuture(finishedIt->second);
    }

    auto pendingIt = PendingResponses_.find(id);
    if (pendingIt != PendingResponses_.end()) {
        LOG_DEBUG("Replying with pending response (MutationId: %v)", id);
        return pendingIt->second;
    }

    YCHECK(PendingResponses_.insert(std::make_pair(id, NewPromise<TSharedRefArray>())).second);

    LOG_DEBUG("Response will be kept (MutationId: %v)", id);

    return TFuture<TSharedRefArray>();
}

void TResponseKeeperBase::EndRequest(
    const TMutationId& id,
    TSharedRefArray data,
    TInstant now,
    bool remember)
{
    YASSERT(id != NullMutationId);

    {
        TGuard<TSpinLock> guard(SpinLock_);

        auto pendingIt = PendingResponses_.find(id);
        if (pendingIt != PendingResponses_.end()) {
            pendingIt->second.Set(data);
        }
        PendingResponses_.erase(pendingIt);

        if (remember) {
            YCHECK(FinishedResponses_.insert(std::make_pair(id, data)).second);
            ResponseEvictionQueue_.push_back(TEvictionItem{id, now});
        }
    }

    if (remember) {
        LOG_DEBUG("Response kept (MutationId: %v)", id);
        UpdateCounters(data, +1);
        OnProfiling();
    }
}

void TResponseKeeperBase::EvictExpiredResponses(TInstant now)
{
    bool changed = false;

    {
        TGuard<TSpinLock> guard(SpinLock_);

        auto deadline = now - Config_->ExpirationTime;
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

void TResponseKeeperBase::Clear()
{
    {
        TGuard<TSpinLock> guard(SpinLock_);
        PendingResponses_.clear();
        FinishedResponses_.clear();
        ResponseEvictionQueue_.clear();
        FinishedResponseSpace_ = 0;
        FinishedResponseCount_ = 0;
    }

    OnProfiling();
}

void TResponseKeeperBase::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    TGuard<TSpinLock> guard(SpinLock_);

    Save(context, FinishedResponses_);

    YCHECK(ResponseEvictionQueue_.size() == FinishedResponses_.size());
    for (const auto& item : ResponseEvictionQueue_) {
        Save(context, item.Id);
        Save(context, item.When);
    }
}

void TResponseKeeperBase::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    TGuard<TSpinLock> guard(SpinLock_);

    Load(context, FinishedResponses_);

    ResponseEvictionQueue_.clear();
    for (int index = 0; index < static_cast<int>(FinishedResponses_.size()); ++index) {
        TEvictionItem item;
        Load(context, item.Id);
        Load(context, item.When);
        ResponseEvictionQueue_.push_back(item);
    }

    FinishedResponseSpace_ = 0;
    FinishedResponseCount_ = 0;
    for (const auto& pair : FinishedResponses_) {
        UpdateCounters(pair.second, +1);
    }
}

void TResponseKeeperBase::UpdateCounters(const TSharedRefArray& data, int delta)
{
    FinishedResponseCount_ += delta;

    for (const auto& part : data) {
        FinishedResponseSpace_ += delta * part.Size();
    }
}

void TResponseKeeperBase::OnProfiling()
{
    Profiler.Aggregate(CountCounter_, FinishedResponseCount_);
    Profiler.Aggregate(SpaceCounter_, FinishedResponseSpace_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
