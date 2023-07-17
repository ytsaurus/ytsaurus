#include "timers.h"
#include "yt.h"

#include <util/digest/multi.h>

template <>
struct THash<NRoren::NPrivate::TTimers::TTimer::TKey>: public NRoren::NPrivate::TTimers::TTimer::TKeyHasher {};
template <>
struct THash<NRoren::NPrivate::TTimers::TTimer::TValue>: public NRoren::NPrivate::TTimers::TTimer::TValueHasher {};

namespace NRoren::NPrivate
{
size_t TTimer::TKeyHasher::operator () (const TKey& key) const
{
    return MultiHash(key.GetKey(), key.GetTimerId(), key.GetCallbackName());
}

size_t TTimer::TValueHasher::operator () (const TValue& value) const
{
    return MultiHash(value.GetTimestamp(), value.GetUserData());
}

size_t TTimer::THasher::operator () (const TTimer& timer) const
{
    return MultiHash(timer.GetKey(), timer.GetValue());
}

bool TTimer::TKeyEqual::operator () (const TTimer::TKey& a, const TTimer::TKey& b)
{
    auto Tie = [] (const TTimer::TKey& key) -> auto {
        return std::tie(key.GetKey(), key.GetTimerId(), key.GetCallbackName());
    };
    return Tie(a) == Tie(b);
}

TTimer::TTimer(TTimerProto timerProto)
    : TTimerProto(std::move(timerProto))
{
}

TTimer::TTimer(const TRawKey& rawKey, const TTimerId& timerId, const TCallbackName& callbackName, const TTimestamp& timestamp, const TUserData& userData)
{
    MutableKey()->SetKey(rawKey);
    MutableKey()->SetTimerId(timerId);
    MutableKey()->SetCallbackName(callbackName);
    MutableValue()->SetTimestamp(timestamp);
    if (userData) {
        MutableValue()->SetUserData(userData.value());
    }
}

bool TTimer::operator == (const TTimer& other) const noexcept
{
    return GetKey().GetKey() == other.GetKey().GetKey()
        && GetKey().GetTimerId() == other.GetKey().GetTimerId()
        && GetKey().GetCallbackName() == other.GetKey().GetCallbackName()
        && GetValue().GetTimestamp() == other.GetValue().GetTimestamp()
        && GetValue().GetUserData() == other.GetValue().GetUserData();
}

bool TTimer::operator < (const TTimer& other) const noexcept
{
    auto Tie = [](const TTimer& timer) noexcept -> auto {
        // similar to g_TimerIndexSchema
        return std::make_tuple(
            timer.GetValue().GetTimestamp(),
            timer.GetKey().GetKey(),
            timer.GetKey().GetTimerId(),
            timer.GetKey().GetCallbackName()
        );
    };
    return Tie(*this) < Tie(other);
}

TTimers::TTimers(const NYT::NApi::IClientPtr ytClient, NYT::NYPath::TYPath ytPath, TTimer::TShardId shardId, TShardProvider shardProvider)
    : YtClient_(ytClient)
    , YTimersPath_(ytPath + "/timers" )
    , YTimersIndexPath_(ytPath + "/timers_index")
    , YTimersMigratePath_(ytPath + "/timers_migrate")
    , ShardId_(shardId)
    , GetShardId_(shardProvider)
{
    CreateTimerTable(YtClient_, YTimersPath_);
    CreateTimerIndexTable(YtClient_, YTimersIndexPath_);
    CreateTimerMigrateTable(YtClient_, YTimersMigratePath_);

    ReInit();
}

void TTimers::ReInit()
{
    const auto guard = RAIILock();
    Y_VERIFY(false == PopulateInProgress_);
    TimerIndex_.clear();
    TimerInFly_.clear();
    DeletedTimers_.clear();
    PopulateIndex();
}

TGuard<TMutex> TTimers::RAIILock()
{
    return TGuard<TMutex>(Mutex_);
}

TTimer TTimers::MergeTimers(const std::optional<TTimer>& oldTimer, const TTimer& newTimer, const TTimer::EMergePolicy policy)
{
    if (!oldTimer) {
        return newTimer;
    }
    TTimer result = newTimer;
    switch (policy) {
        case TTimer::EMergePolicy::REPLACE:
            break;
        case TTimer::EMergePolicy::MIN:
            if (oldTimer->GetValue().GetTimestamp() != 0) {
                result.MutableValue()->SetTimestamp(Min(oldTimer->GetValue().GetTimestamp(), newTimer.GetValue().GetTimestamp()));
            break;
            }
        case TTimer::EMergePolicy::MAX:
            result.MutableValue()->SetTimestamp(Max(oldTimer->GetValue().GetTimestamp(), newTimer.GetValue().GetTimestamp()));
        }
    return result;
}

void TTimers::Commit(const NYT::NApi::ITransactionPtr tx, const TTimers::TTimersHashMap& updates)
{
    TVector<TTimer::TKey> keys;
    keys.reserve(updates.size());
    for (const auto& [key, timerAndPolicy] : updates) {
        keys.emplace_back(key);
    }

    THashMap<TTimer::TKey, TTimer, TTimer::TKeyHasher, TTimer::TKeyEqual> existsTimers;
    existsTimers.reserve(updates.size());
    for (auto& timer : YtLookupTimers(tx, keys)) {
        existsTimers.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(timer.GetKey()),
            std::forward_as_tuple(std::move(timer))
        );
    }

    const auto guard = RAIILock();
    for (auto& [key, timerAndPolicy] : updates) {
        auto& [newTimer, policy] = timerAndPolicy;
        std::optional<std::reference_wrapper<const TTimer>> oldTimer;
        if (existsTimers.contains(key)) {
            oldTimer = std::cref(existsTimers.at(key));
        }
        const auto targetTimer = MergeTimers(oldTimer, newTimer, policy);
        if (oldTimer && targetTimer == *oldTimer) {
            continue;
        }
        YtDeleteTimer(tx, key);
        if (oldTimer) {
            YtDeleteIndex(tx, oldTimer.value());
            DeletedTimers_.emplace(oldTimer.value());
            TimerIndex_.erase(oldTimer.value());
            TimerInFly_.erase(oldTimer.value());
        }
        if (targetTimer.GetValue().GetTimestamp() != 0) {
            YtInsertTimer(tx, targetTimer);
            YtInsertIndex(tx, targetTimer);
            if (!TimerIndex_ || newTimer < *(--TimerIndex_.end())) {
                TimerIndex_.insert(newTimer);
                Cleanup();
            }
        }
    }
}

void TTimers::OnCommit()
{
    const auto guard = RAIILock();
    PopulateIndex();
}

TVector<TTimer> TTimers::GetReadyTimers(size_t limit)
{
    TVector<TTimer> result;
    for (const auto& timer : TimerIndex_) {
        if (0 == limit) {
            break;
        }
        if (TimerInFly_.contains(timer)) {
            continue;
        }
        TimerInFly_.insert(timer);
        result.push_back(timer);
        --limit;
    }
    return result;
}

bool TTimers::IsValidForExecute(const TTimer& timer, const bool isTimerChanged)
{
    return TimerInFly_.contains(timer) && !isTimerChanged;
}

void TTimers::Cleanup()
{
    while (TimerIndex_.size() > IndexLimit_) {
        TimerIndex_.erase(--TimerIndex_.end());
    }
}

void TTimers::Migrate(const TTimer& timer, const TTimer::TShardId shardId)
{
    auto tx = NYT::NConcurrency::WaitFor(YtClient_->StartTransaction(NYT::NTransactionClient::ETransactionType::Tablet)).ValueOrThrow();
    const auto timers = YtLookupTimers(tx, {timer.GetKey()});
    if (!timers.empty() && (timers.front() == timer)) {
        YtInsertMigrate(tx, timer, shardId);
    }
    tx->Commit();
}

void TTimers::PopulateIndex()
{
    //Skip populate if index is full
    if ((IndexLimit_ - TimerIndex_.size()) < IndexSelectBatch_) {
        return;
    }

    bool expected = false;
    if (!PopulateInProgress_.compare_exchange_strong(expected, true)) {
        return;
    }

    try {
        DeletedTimers_.clear();
        auto topTimers = YtSelectIndex();
        for (auto& timer : topTimers) {
            if (DeletedTimers_.contains(timer)) {
                continue;
            }
            const TTimer::TShardId trueShardId = GetShardId_(timer.GetKey().GetKey());
            if (ShardId_ != trueShardId) {
                Migrate(timer, trueShardId);
            } else {
                TimerIndex_.insert(timer);
            }
        }
    } catch (...) {
        PopulateInProgress_.store(false);
        throw;
    }
    PopulateInProgress_.store(false);
}

TVector<TTimer> TTimers::YtSelectIndex()
{
    return NPrivate::YtSelectIndex(YtClient_, YTimersIndexPath_, ShardId_, TimerIndex_.size(), IndexLimit_ - TimerIndex_.size());
}

TVector<TTimer> TTimers::YtSelectMigrate()
{
    return NPrivate::YtSelectMigrate(YtClient_, YTimersMigratePath_, ShardId_, IndexSelectBatch_);
}

TVector<TTimer> TTimers::YtLookupTimers(const NYT::NApi::IClientBasePtr tx, const TVector<TTimer::TKey>& keys)
{
    return NPrivate::YtLookupTimers(tx, YTimersPath_, keys);
}

void TTimers::YtInsertMigrate(const NYT::NApi::ITransactionPtr tx, const TTimer& timer, const TTimer::TShardId shardId)
{
    NPrivate::YtInsertMigrate(tx, YTimersMigratePath_, timer, shardId);
}

void TTimers::YtInsertTimer(const NYT::NApi::ITransactionPtr tx, const TTimer& timer)
{
    NPrivate::YtInsertTimer(tx, YTimersPath_, timer);
}

void TTimers::YtInsertIndex(const NYT::NApi::ITransactionPtr  tx, const TTimer& timer)
{
    NPrivate::YtInsertIndex(tx, YTimersIndexPath_, timer, GetShardId_(timer.GetKey().GetKey()));
}

void TTimers::YtDeleteTimer(const NYT::NApi::ITransactionPtr tx, const TTimer::TKey& key)
{
    NPrivate::YtDeleteTimer(tx, YTimersPath_, key);
}

void TTimers::YtDeleteIndex(const NYT::NApi::ITransactionPtr tx, const TTimer& timer)
{
    NPrivate::YtDeleteIndex(tx, YTimersIndexPath_, timer, GetShardId_(timer.GetKey().GetKey()));
}

}  // namespace NRoren::NPrivate

