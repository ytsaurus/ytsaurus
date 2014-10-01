#include "stdafx.h"
#include "response_keeper_detail.h"
#include "config.h"

#include <core/misc/serialize.h>

namespace NYT {
namespace NRpc {

using namespace NConcurrency;

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
        return MakeFuture(finishedIt->second);
    }

    auto pendingIt = PendingResponses_.find(id);
    if (pendingIt != PendingResponses_.end()) {
        return pendingIt->second;
    }

    YCHECK(PendingResponses_.insert(std::make_pair(id, NewPromise<TSharedRefArray>())).second);

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
            auto finishedPair = FinishedResponses_.insert(std::make_pair(id, data));
            YCHECK(finishedPair.second);

            TItem item;
            item.Iterator = finishedPair.first;
            item.When = now;
            ResponseEvictionQueue_.push_back(item);
        }
    }

    if (remember) {
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

            UpdateCounters(item.Iterator->second, -1);
            FinishedResponses_.erase(item.Iterator);
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

        for (auto& pair : PendingResponses_) {
            pair.second.Cancel();
        }
        PendingResponses_.clear();
        FinishedResponses_.clear();
        ResponseEvictionQueue_.clear();
        FinishedResponseSpace_ = 0;
    }

    OnProfiling();
}

void TResponseKeeperBase::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    TGuard<TSpinLock> guard(SpinLock_);

    Save(context, FinishedResponses_);

    for (const auto& item : ResponseEvictionQueue_) {
        Save(context, item.Iterator->first);
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
        TItem item;
        auto id = Load<TMutationId>(context);
        item.Iterator = FinishedResponses_.find(id);
        YCHECK(item.Iterator != FinishedResponses_.end());
        item.When = Load<TInstant>(context);
    }

    FinishedResponseSpace_ = 0;
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
