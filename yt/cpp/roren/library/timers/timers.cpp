#include "timers.h"
#include "yt.h"

#include <util/digest/multi.h>

template <>
struct THash<NRoren::NPrivate::TTimer::TKey>: public NRoren::NPrivate::TTimer::TKeyHasher {};
template <>
struct THash<NRoren::NPrivate::TTimer::TValue>: public NRoren::NPrivate::TTimer::TValueHasher {};

namespace NRoren::NPrivate
{
size_t TTimer::TKeyHasher::operator () (const TKey& key) const
{
    return MultiHash(key.GetKey(), key.GetTimerId(), key.GetCallbackId());
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
        return std::tie(key.GetKey(), key.GetTimerId(), key.GetCallbackId());
    };
    return Tie(a) == Tie(b);
}

TTimer::TTimer(TTimerProto timerProto)
    : TTimerProto(std::move(timerProto))
{
}

TTimer::TTimer(const TRawKey& rawKey, const TTimerId& timerId, const TCallbackId& callbackId, const TTimestamp& timestamp, const TUserData& userData)
{
    MutableKey()->SetKey(rawKey);
    MutableKey()->SetTimerId(timerId);
    MutableKey()->SetCallbackId(callbackId);
    MutableValue()->SetTimestamp(timestamp);
    if (userData) {
        MutableValue()->SetUserData(userData.value());
    }
}

bool TTimer::operator == (const TTimer& other) const noexcept
{
    return GetValue().GetTimestamp() == other.GetValue().GetTimestamp()
        && GetKey().GetKey() == other.GetKey().GetKey()
        && GetKey().GetTimerId() == other.GetKey().GetTimerId()
        && GetKey().GetCallbackId() == other.GetKey().GetCallbackId()
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
            timer.GetKey().GetCallbackId()
        );
    };
    return Tie(*this) < Tie(other);
}

TTimersContainer::TGuard TTimersContainer::GetLock()
{
    return TGuard(Lock_);
}

void TTimersContainer::Clear(const TGuard& lock)
{
    Y_UNUSED(lock);
    TimersIndex_.clear();
    TimersNotInFly_.clear();
    TimersInFly_.clear();
    DeletedTimers_.clear();
}

void TTimersContainer::Insert(const TGuard& lock, TTimer timer)
{
    Y_UNUSED(lock);
    auto [it, isInserted] = TimersIndex_.insert(std::move(timer));
    TimersNotInFly_.insert(it);
}

bool TTimersContainer::InsertTop(const TGuard& lock, TTimer timer)
{
    const bool inRange = TimersIndex_.empty() || timer < *std::prev(TimersIndex_.end());
    if (inRange) {
        TTimersContainer::Insert(lock, std::move(timer));
    }
    return inRange;
}

void TTimersContainer::Delete(const TGuard& lock, const TTimer& timer)
{
    Y_UNUSED(lock);
    DeletedTimers_.emplace(timer);
    auto it = TimersIndex_.find(timer);
    if (it != TimersIndex_.end()) {
        if (0 == TimersInFly_.erase(timer)) {
            TimersNotInFly_.erase(it);
        }
        TimersIndex_.erase(it);
    }
}

void TTimersContainer::Cleanup(const TGuard& lock, size_t limit)
{
    Y_UNUSED(lock);
    const ssize_t n = TimersIndex_.size() - limit;
    if (n <= 0) {
        return;
    }

    auto it = TimersIndex_.end();
    std::advance(it, -n);
    TimersNotInFly_.erase(TimersNotInFly_.find(it), TimersNotInFly_.end());
    TimersIndex_.erase(it, TimersIndex_.end());
}

size_t TTimersContainer::GetIndexSize(const TGuard& lock) const
{
    Y_UNUSED(lock);
    return TimersIndex_.size();
}

void TTimersContainer::ResetDeletedTimers(const TGuard& lock)
{
    Y_UNUSED(lock);
    DeletedTimers_.clear();
}

bool TTimersContainer::IsDeleted(const TGuard& lock, const TTimer& timer) const
{
    Y_UNUSED(lock);
    return DeletedTimers_.contains(timer);
}

TVector<TTimer> TTimersContainer::GetReadyTimers(size_t limit)
{
    const auto lock = GetLock();
    return GetReadyTimers(lock, limit);
}

TVector<TTimer> TTimersContainer::GetReadyTimers(const TGuard& lock, size_t limit)
{
    Y_UNUSED(lock);
    TVector<TTimer> result;
    limit = MIN(limit, TimersNotInFly_.size());
    const auto now = TInstant::Now();
    auto end = TimersNotInFly_.begin();
    for (; limit > 0; --limit, ++end) {
        const TTimer& timer = **end;
        if (TInstant::Seconds(timer.GetValue().GetTimestamp()) > now) {
            break;
        }
    }
    for (auto it = TimersNotInFly_.begin(); it != end; ++it) {
        const TTimer& timer = **it;
        result.push_back(timer);
        TimersInFly_.insert(timer);
    }
    TimersNotInFly_.erase(TimersNotInFly_.begin(), end);
    return result;
}

bool TTimersContainer::IsValidForExecute(const TTimer& timer, const bool isTimerChanged)
{
    const auto lock = GetLock();
    return IsValidForExecute(lock, timer, isTimerChanged);
}

bool TTimersContainer::IsValidForExecute(const TGuard& lock, const TTimer& timer, const bool isTimerChanged)
{
    Y_UNUSED(lock);
    return !isTimerChanged && TimersInFly_.contains(timer);
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
    {
        const auto lock = GetLock();
        Y_VERIFY(false == PopulateInProgress_);
        TTimersContainer::Clear(lock);
    }
    PopulateIndex();
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

    const auto lock = GetLock();
    for (auto& [key, timerAndPolicy] : updates) {
        const auto& [newTimer, policy] = timerAndPolicy;
        std::optional<std::reference_wrapper<const TTimer>> oldTimer;
        if (existsTimers.contains(key)) {
            oldTimer = std::cref(existsTimers.at(key));
        }
        auto targetTimer = MergeTimers(oldTimer, newTimer, policy);
        if (oldTimer && targetTimer == *oldTimer) {
            continue;
        }
        if (oldTimer) {
            YtDeleteTimer(tx, key);
            YtDeleteIndex(tx, oldTimer.value());
            TTimersContainer::Delete(lock, oldTimer.value());
        }
        if (targetTimer.GetValue().GetTimestamp() != 0) {
            YtInsertTimer(tx, targetTimer);
            YtInsertIndex(tx, targetTimer);
            TTimersContainer::InsertTop(lock, std::move(targetTimer));
        }
    }
    Cleanup(lock, IndexLimit_);
}

void TTimers::OnCommit()
{
    PopulateIndex();
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
    size_t offset = 0;
    {
        const auto lock = GetLock();
        if (SkipPopulateUntil_ >= TInstant::Now()) {
            return;
        }

        bool expected = false;
        if (!PopulateInProgress_.compare_exchange_strong(expected, true)) {
            return;
        }

        ResetDeletedTimers(lock);
        offset = TTimersContainer::GetIndexSize(lock);
    }

    try {
        auto selectedTimers = YtSelectIndex(offset);

        const auto lock = GetLock();
        if (selectedTimers.empty()) {
            SkipPopulateUntil_ = TInstant::Now() + TDuration::Seconds(1);
        }
        for (auto& timer : selectedTimers) {
            if (IsDeleted(lock, timer)) {
                continue;
            }
            const TTimer::TShardId trueShardId = GetShardId_(timer.GetKey().GetKey());
            if (ShardId_ != trueShardId) {
                Migrate(timer, trueShardId);
            } else {
                TTimersContainer::Insert(lock, timer);
            }
        }
        Cleanup(lock, IndexLimit_);
    } catch (...) {
        PopulateInProgress_.store(false);
        throw;
    }
    PopulateInProgress_.store(false);
}

TVector<TTimer> TTimers::YtSelectIndex(const size_t offset)
{
    const size_t limit = MIN(IndexSelectBatch_, IndexLimit_ - offset);
    if (limit == 0) {
        return {};
    }
    return NPrivate::YtSelectIndex(YtClient_, YTimersIndexPath_, ShardId_, offset, limit);
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

