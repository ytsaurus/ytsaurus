#pragma once

#ifndef PERSISTED_STATE_H_
    #error "Direct inclusion of this file is not allowed, include persisted_state.h"
    // For the sake of sane code completion.
    #include "persisted_state.h"
#endif

#include <yt/yt/core/misc/finally.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TPersistedStateTransactionContext<TKey, TValue>::TPersistedStateTransactionContext(TPersistedStateRecordSet<TKey, TValue>& recordSet, bool readOnly)
    : RecordSet(recordSet)
    , ReadAll(readOnly) // Set ReadAll for read-only in order not to collect ReadSet anymore; this does not contradict anything.
{
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
const std::pair<const TKey&, const TValue&>* TPersistedState<TKey, TValue>::TTransactionalIterator::TMemberAccess::operator->() const
{
    return this;
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TIterator::TIterator(TRecordSet::const_iterator it, TRecordSet::const_iterator end)
    : TRecordSet::const_iterator(it)
    , End_(end)
{
    if (it != End_) {
        const TRecordPtr& record = TRecordSet::iterator::operator*();
        YT_ASSERT(record->Value.has_value());
        Storage_.emplace(record->Interval.EffectiveLeft().GetKeySure(), *record->Value);
    }
}

template <class TKey, class TValue>
const std::pair<const TKey, const TValue>& TPersistedState<TKey, TValue>::TIterator::operator*() const
{
    return *Storage_;
}

template <class TKey, class TValue>
const std::pair<const TKey, const TValue>* TPersistedState<TKey, TValue>::TIterator::operator->() const
{
    return &*Storage_;
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TIterator& TPersistedState<TKey, TValue>::TIterator::operator++()
{
    YT_ASSERT(TRecordSet::iterator::operator*()->Value.has_value());
    TRecordSet::iterator::operator++();
    YT_ASSERT(!TRecordSet::iterator::operator*()->Value.has_value());
    TRecordSet::iterator::operator++();
    if (*this != End_) {
        const TRecordPtr& record = TRecordSet::iterator::operator*();
        YT_ASSERT(record->Value.has_value());
        Storage_.emplace(record->Interval.EffectiveLeft().GetKeySure(), *record->Value);
    }
    return *this;
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TIterator TPersistedState<TKey, TValue>::TIterator::operator++(int)
{
    TIterator ret = *this;
    YT_ASSERT(TRecordSet::iterator::operator*().Value.has_value());
    TRecordSet::iterator::operator++();
    YT_ASSERT(!TRecordSet::iterator::operator*().Value.has_value());
    TRecordSet::iterator::operator++();
    return ret;
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TTransactionalIterator::TTransactionalIterator(const TCommon& common, TWriteSet& writeSet)
    : Common_{common}
    , WriteSet_{writeSet}
    , CommonIterator_{common.begin()}
    , WriteSetIterator_{writeSet.begin()}
{
    YT_ASSERT(Common_.size() > 0);
    if (Common_.size() == 1) {
        CommonIterator_ = Common_.end();
    }
    if (CommonIterator_ != Common_.end()) {
        YT_ASSERT(!(*CommonIterator_)->Value.has_value());
        ++CommonIterator_;
        YT_ASSERT(CommonIterator_ != Common_.end());
        YT_ASSERT((*CommonIterator_)->Value.has_value());
        if (WriteSet_.contains((*CommonIterator_)->Interval.EffectiveLeft().GetKeySure())) {
            operator++();
        }
    } else if (WriteSetIterator_ != WriteSet_.end()) {
        if (!WriteSetIterator_->second.has_value()) {
            operator++();
        }
    }
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TTransactionalIterator::TTransactionalIterator(const TCommon& common, TWriteSet& writeSet, TWriteSetIterator writeSetIterator)
    : Common_{common}
    , WriteSet_{writeSet}
    , CommonIterator_{Common_.end()}
    , WriteSetIterator_{writeSetIterator}
{ }

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TTransactionalIterator::TTransactionalIterator(
    const TCommon& common,
    TWriteSet& writeSet,
    TCommonIterator commonIterator,
    TWriteSetIterator writeSetIterator,
    ELookupStatus status)
    : Common_{common}
    , WriteSet_{writeSet}
    , CommonIterator_{status == ELookupStatus::RecordSetFound ? commonIterator : Common_.end()}
    , WriteSetIterator_{
        status == ELookupStatus::RecordSetFound
            ? writeSet.begin()
            : (status == ELookupStatus::WriteSetInserted
                    ? writeSetIterator
                    : writeSet.end())}
{ }

template <class TKey, class TValue>
std::pair<const TKey&, const TValue&> TPersistedState<TKey, TValue>::TTransactionalIterator::operator*() const
{
    if (CommonIterator_ != Common_.end()) {
        YT_ASSERT((*CommonIterator_)->Value.has_value());
        const TRecordPtr& record = *CommonIterator_;
        return {record->Interval.EffectiveLeft().GetKeySure(), *record->Value};
    } else {
        YT_ASSERT(WriteSetIterator_ != WriteSet_.end());
        YT_ASSERT(WriteSetIterator_->second.has_value());
        return {WriteSetIterator_->first, WriteSetIterator_->second.value()};
    }
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TTransactionalIterator::TMemberAccess TPersistedState<TKey, TValue>::TTransactionalIterator::operator->() const
{
    auto pair = operator*();
    return {pair.first, pair.second};
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TTransactionalIterator& TPersistedState<TKey, TValue>::TTransactionalIterator::operator++()
{
    if (CommonIterator_ != Common_.end()) {
        while (true) {
            YT_ASSERT((*CommonIterator_)->Value.has_value());
            ++CommonIterator_;
            YT_ASSERT(!(*CommonIterator_)->Value.has_value());
            ++CommonIterator_;
            if (CommonIterator_ == Common_.end()) {
                break;
            }
            YT_ASSERT((*CommonIterator_)->Value.has_value());
            if (!WriteSet_.contains((*CommonIterator_)->Interval.EffectiveLeft().GetKeySure())) {
                return *this;
            }
        }
        while (WriteSetIterator_ != WriteSet_.end()) {
            if (WriteSetIterator_->second.has_value()) {
                break;
            }
            ++WriteSetIterator_;
        }
    } else {
        YT_ASSERT(WriteSetIterator_ != WriteSet_.end());
        while (++WriteSetIterator_ != WriteSet_.end()) {
            if (WriteSetIterator_->second.has_value()) {
                break;
            }
        }
    }
    return *this;
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TTransactionalIterator TPersistedState<TKey, TValue>::TTransactionalIterator::operator++(int)
{
    TTransactionalIterator ret = *this;
    operator++();
    return ret;
}

template <class TKey, class TValue>
bool TPersistedState<TKey, TValue>::TTransactionalIterator::operator==(const TTransactionalIterator& that) const
{
    return CommonIterator_ == that.CommonIterator_ && WriteSetIterator_ == that.WriteSetIterator_;
}

template <class TKey, class TValue>
bool TPersistedState<TKey, TValue>::TTransactionalIterator::operator==(const TIterator& that) const
{
    return CommonIterator_ == that;
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
constexpr bool TPersistedStateRecordLess<TKey, TValue>::operator()(const TRecord& lhs, const TRecord& rhs) const
{
    return lhs->Interval.EffectiveLeft() < rhs->Interval.EffectiveLeft();
}

template <class TKey, class TValue>
constexpr bool TPersistedStateRecordLess<TKey, TValue>::operator()(const TRecord& lhs, const TEndpoint<TKey>& rhs) const
{
    return lhs->Interval.EffectiveLeft() < rhs;
}

template <class TKey, class TValue>
constexpr bool TPersistedStateRecordLess<TKey, TValue>::operator()(const TEndpoint<TKey>& lhs, const TRecord& rhs) const
{
    return lhs < rhs->Interval.EffectiveLeft();
}

template <class TKey, class TValue>
constexpr bool TPersistedStateRecordLess<TKey, TValue>::operator()(const TRecord& lhs, const TKey& rhs) const
{
    return lhs->Interval.EffectiveLeft() < rhs;
}

template <class TKey, class TValue>
constexpr bool TPersistedStateRecordLess<TKey, TValue>::operator()(const TKey& lhs, const TRecord& rhs) const
{
    return lhs < rhs->Interval.EffectiveLeft();
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TPersistedState()
{
    // Create the one and only fake record.
    RecordSet_.insert(CreateRecord(FakeSequenceId, TOptKey{std::nullopt}, TOptKey{std::nullopt}, TOptValue{std::nullopt}));
}

template <class TKey, class TValue>
bool TPersistedState<TKey, TValue>::contains(const TKey& key) const
{
    auto guard = TransactionGuard();
    return RecordSet_.contains(key);
}

template <class TKey, class TValue>
TValue TPersistedState<TKey, TValue>::at(const TKey& key) const
{
    auto guard = TransactionGuard();
    auto it = std::as_const(RecordSet_).find(key);
    if (it == std::as_const(RecordSet_).end()) {
        throw std::out_of_range("TPersistedState::at: out of range");
    } else {
        YT_ASSERT((*it)->Value.has_value());
    }
    return *(*it)->Value;
}

template <class TKey, class TValue>
const TValue* TPersistedState<TKey, TValue>::FindPtr(const TKey& key) const
{
    auto guard = TransactionGuard();
    auto it = std::as_const(RecordSet_).find(key);
    if (it == std::as_const(RecordSet_).end()) {
        return nullptr;
    } else {
        YT_ASSERT((*it)->Value.has_value());
        return &*(*it)->Value;
    }
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::iterator TPersistedState<TKey, TValue>::find(const TKey& key) const
{
    auto guard = TransactionGuard();
    return {std::as_const(RecordSet_).find(key), std::as_const(RecordSet_).end()};
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::iterator TPersistedState<TKey, TValue>::begin() const
{
    auto it = std::as_const(RecordSet_).begin();
    if (it != std::as_const(RecordSet_).end()) {
        ++it;
    }
    return {it, std::as_const(RecordSet_).end()};
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::iterator TPersistedState<TKey, TValue>::end() const
{
    return {std::as_const(RecordSet_).end(), std::as_const(RecordSet_).end()};
}

template <class TKey, class TValue>
size_t TPersistedState<TKey, TValue>::size() const
{
    auto guard = TransactionGuard();
    return RecordSet_.size() / 2;
}

template <class TKey, class TValue>
template <std::convertible_to<TKey> TKeyRef, std::convertible_to<TValue> TValueRef>
std::pair<typename TPersistedState<TKey, TValue>::iterator, bool> TPersistedState<TKey, TValue>::insert_or_assign(
    TKeyRef&& key,
    TValueRef&& value)
{
    auto tx = StartTransaction();
    auto result = insert_or_assign(tx, key, std::forward<TValueRef>(value));
    tx->Commit();
    return std::pair(iterator(RecordSet_.find(key), RecordSet_.end()), result.second);
}

template <class TKey, class TValue>
template <class... T>
std::pair<typename TPersistedState<TKey, TValue>::iterator, bool> TPersistedState<TKey, TValue>::emplace(T&&... t)
{
    auto&& [key, value] = std::pair<const TKey, TValue>{std::forward<T>(t)...};
    auto it = RecordSet_.find(key);
    if (it != RecordSet_.end()) {
        // Insertion is blocked.
        return {iterator(it, RecordSet_.end()), false};
    }
    return insert_or_assign(std::move(key), std::move(value));
}

template <class TKey, class TValue>
size_t TPersistedState<TKey, TValue>::erase(const TKey& key)
{
    auto tx = StartTransaction();
    auto result = erase(tx, key);
    tx->Commit();
    return result;
}

template <class TKey, class TValue>
TPersistedStateTransactionContext<TKey, TValue>& TPersistedState<TKey, TValue>::GetTransactionContext(
    TPersistedStateTransactionPtr& tx,
    bool forModify)
{
    THROW_ERROR_EXCEPTION_IF(!tx, "Transaction must be started");
    THROW_ERROR_EXCEPTION_UNLESS(
        tx->State_ == EPersistedStateTransactionState::Active,
        "Operation over state %v",
        TransactionDenyReason(tx));
    THROW_ERROR_EXCEPTION_IF(forModify && IsReadOnly_, "Can't modify state: is either not leader or not recovered yet");
    return static_cast<TPersistedStateTransactionContext<TKey, TValue>&>(GetTransactionContextBase(tx));
}

template <class TKey, class TValue>
const char* TPersistedState<TKey, TValue>::TransactionDenyReason(const TPersistedStateTransactionPtr& tx)
{
    switch (tx->State_) {
        case EPersistedStateTransactionState::Active:
            return "is OK";
        case EPersistedStateTransactionState::Conflicted:
            return "has been interrupted due to conflict";
        case EPersistedStateTransactionState::Prepared:
        case EPersistedStateTransactionState::Committed:
            return "has been already committed";
        case EPersistedStateTransactionState::Reverted:
            return "was failed to persist";
        case EPersistedStateTransactionState::Aborted:
            return "has been cancelled";
        default:
            YT_UNREACHABLE();
    };
}

template <class TKey, class TValue>
void TPersistedState<TKey, TValue>::TrackRead(TPersistedStateTransactionPtr& tx, const TKey& key)
{
    YT_ASSERT(tx);
    find(tx, key);
}

template <class TKey, class TValue>
void TPersistedState<TKey, TValue>::TrackRead(TPersistedStateTransactionPtr& tx)
{
    YT_ASSERT(tx);
    auto& context = GetTransactionContext(tx, /*forModify*/ false);
    TrackRead(context);
}

template <class TKey, class TValue>
void TPersistedState<TKey, TValue>::TrackRead(
    TPersistedStateTransactionContext<TKey, TValue>& context,
    const TKey& key,
    std::optional<TSequenceId> readResult)
{
    if (context.ReadAll) {
        return;
    }
    context.ReadSet.emplace(key, readResult);
}

template <class TKey, class TValue>
void TPersistedState<TKey, TValue>::TrackRead(TPersistedStateTransactionContext<TKey, TValue>& context)
{
    if (context.ReadAll) {
        return;
    }
    context.ReadSet.clear();
    context.ReadAll = true;
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TLookupResult TPersistedState<TKey, TValue>::Lookup(
    TPersistedStateTransactionContext<TKey, TValue>& context,
    const TKey& key,
    bool trackRead)
{
    if (auto it = context.WriteSet.find(key); it != context.WriteSet.end()) {
        return {it, context.RecordSet.end(), it->second.has_value() ? ELookupStatus::WriteSetInserted : ELookupStatus::WriteSetErased};
    }
    auto it = context.RecordSet.find(key);
    if (trackRead) {
        TrackRead(context, key, it != context.RecordSet.end() ? std::optional((*it)->SequenceId) : std::nullopt);
    }
    return {context.WriteSet.end(), it, it != context.RecordSet.end() ? ELookupStatus::RecordSetFound : ELookupStatus::RecordSetNotFound};
}

template <class TKey, class TValue>
template <std::convertible_to<TKey> TKeyRef, std::convertible_to<std::optional<TValue>> TOptValueRef>
void TPersistedState<TKey, TValue>::UpdateWriteSet(TPersistedStateTransactionContext<TKey, TValue>& context, TWriteSetIterator& it, ELookupStatus lookupStatus, TKeyRef&& key, TOptValueRef&& value)
{
    if (it != context.WriteSet.end()) {
        it->second = std::forward<TOptValueRef>(value);
    } else {
        it = EmplaceOrCrash(context.WriteSet, std::forward<TKeyRef>(key), std::forward<TOptValueRef>(value));
    }
    bool wasPresent = lookupStatus == ELookupStatus::WriteSetInserted || lookupStatus == ELookupStatus::RecordSetFound;
    bool nowPresent = it->second.has_value();
    context.RecordSetSizeDiff -= wasPresent;
    context.RecordSetSizeDiff += nowPresent;
}

template <class TKey, class TValue>
bool TPersistedState<TKey, TValue>::contains(TPersistedStateTransactionPtr& tx, const TKey& key)
{
    if (tx) {
        auto& context = GetTransactionContext(tx, /*forModify*/ false);
        if (auto it = context.WriteSet.find(key); it != context.WriteSet.end()) {
            return it->second.has_value();
        }
        auto it = context.RecordSet.find(key);
        if (it != context.RecordSet.end()) {
            YT_ASSERT((*it)->Value.has_value());
        }
        TrackRead(context, key, it != context.RecordSet.end() ? std::optional((*it)->SequenceId) : std::nullopt);
        return it != context.RecordSet.end();
    }
    return contains(key);
}

template <class TKey, class TValue>
TValue TPersistedState<TKey, TValue>::at(TPersistedStateTransactionPtr& tx, const TKey& key)
{
    if (tx) {
        auto& context = GetTransactionContext(tx, /*forModify*/ false);
        if (auto it = context.WriteSet.find(key); it != context.WriteSet.end()) {
            if (!it->second.has_value()) {
                throw std::out_of_range("TPersistedState::at: out of range");
            }
            return it->second.value();
        }
        auto it = context.RecordSet.find(key);
        TrackRead(context, key, it != context.RecordSet.end() ? std::optional((*it)->SequenceId) : std::nullopt);
        if (it == context.RecordSet.end()) {
            throw std::out_of_range("TPersistedState::at: out of range");
        }
        YT_ASSERT((*it)->Value.has_value());
        return *(*it)->Value;
    }
    return at(key);
}

template <class TKey, class TValue>
const TValue* TPersistedState<TKey, TValue>::FindPtr(TPersistedStateTransactionPtr& tx, const TKey& key)
{
    if (tx) {
        auto& context = GetTransactionContext(tx, /*forModify*/ false);
        if (auto it = context.WriteSet.find(key); it != context.WriteSet.end()) {
            return it->second.has_value() ? &it->second.value() : nullptr;
        }
        auto it = context.RecordSet.find(key);
        TrackRead(context, key, it != context.RecordSet.end() ? std::optional((*it)->SequenceId) : std::nullopt);
        if (it == context.RecordSet.end()) {
            return nullptr;
        } else {
            YT_ASSERT((*it)->Value.has_value());
            return &*(*it)->Value;
        }
    }
    return FindPtr(key);
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TTransactionalIterator TPersistedState<TKey, TValue>::find(TPersistedStateTransactionPtr& tx, const TKey& key)
{
    if (tx) {
        auto& context = GetTransactionContext(tx, /*forModify*/ false);
        auto&& [itWriteSet, itRecordSet, status] = Lookup(context, key, /*trackRead*/ true);
        return {context.RecordSet, context.WriteSet, itRecordSet, itWriteSet, status};
    } else {
        auto guard = TransactionGuard();
        auto it = std::as_const(RecordSet_).find(key);
        auto status = it != std::as_const(RecordSet_).end() ? ELookupStatus::RecordSetFound : ELookupStatus::RecordSetNotFound;
        return {RecordSet_, FakeWriteSet_, it, FakeWriteSet_.end(), status};
    }
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TTransactionalIterator TPersistedState<TKey, TValue>::begin(TPersistedStateTransactionPtr& tx)
{
    if (tx) {
        auto& context = GetTransactionContext(tx, /*forModify*/ false);
        TrackRead(context);
        return {context.RecordSet, context.WriteSet};
    } else {
        return {RecordSet_, FakeWriteSet_};
    }
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TTransactionalIterator TPersistedState<TKey, TValue>::end(TPersistedStateTransactionPtr& tx)
{
    if (tx) {
        auto& context = GetTransactionContext(tx, /*forModify*/ false);
        return {context.RecordSet, context.WriteSet, context.WriteSet.end()};
    } else {
        return {RecordSet_, FakeWriteSet_, FakeWriteSet_.end()};
    }
}

template <class TKey, class TValue>
size_t TPersistedState<TKey, TValue>::size(TPersistedStateTransactionPtr& tx)
{
    if (tx) {
        auto& context = GetTransactionContext(tx, /*forModify*/ false);
        // Strictly speaking we read entire state here and should add it to tx's read set.
        // On the other hand that would lead to destructive conflicts, while transaction that depends on state size is strange.
        return context.RecordSet.size() / 2 + context.RecordSetSizeDiff;
    } else {
        return size();
    }
}

template <class TKey, class TValue>
template <std::convertible_to<TKey> TKeyRef, std::convertible_to<TValue> TValueRef>
std::pair<typename TPersistedState<TKey, TValue>::TTransactionalIterator, bool> TPersistedState<TKey, TValue>::insert_or_assign(TPersistedStateTransactionPtr& tx, TKeyRef&& key, TValueRef&& value)
{
    THROW_ERROR_EXCEPTION_IF(tx->IsReadOnly_, "Transaction over state is read-only, modification is prohibited");
    tx->HasNoWrites_ = false;
    auto& context = GetTransactionContext(tx, /*forModify*/ true);
    // Do not add to read set in order to avoid conflict in the most common case when isInserted flag is not used.
    auto&& [itWriteSet, itRecordSet, status] = Lookup(context, key, /*trackRead*/ false);
    UpdateWriteSet(context, itWriteSet, status, std::forward<TKeyRef>(key), std::forward<TValueRef>(value));
    bool isInserted = status == ELookupStatus::WriteSetErased || status == ELookupStatus::RecordSetNotFound;
    return {TTransactionalIterator{RecordSet_, context.WriteSet, itWriteSet}, isInserted};
}

template <class TKey, class TValue>
template <class... T>
std::pair<typename TPersistedState<TKey, TValue>::TTransactionalIterator, bool> TPersistedState<TKey, TValue>::emplace(TPersistedStateTransactionPtr& tx, T&&... t)
{
    THROW_ERROR_EXCEPTION_IF(tx->IsReadOnly_, "Transaction over state is read-only, modification is prohibited");
    tx->HasNoWrites_ = false;
    auto&& [key, value] = std::pair<const TKey, TValue>{std::forward<T>(t)...};
    auto& context = GetTransactionContext(tx, /*forModify*/ true);
    auto&& [itWriteSet, itRecordSet, status] = Lookup(context, key, /*trackRead*/ true);
    if (status == ELookupStatus::WriteSetInserted || status == ELookupStatus::RecordSetFound) {
        return {TTransactionalIterator{RecordSet_, context.WriteSet, itRecordSet, itWriteSet, status}, false};
    }
    UpdateWriteSet(context, itWriteSet, status, std::move(key), std::move(value));
    return {TTransactionalIterator{RecordSet_, context.WriteSet, itWriteSet}, true};
}

template <class TKey, class TValue>
size_t TPersistedState<TKey, TValue>::erase(TPersistedStateTransactionPtr& tx, const TKey& key)
{
    THROW_ERROR_EXCEPTION_IF(tx->IsReadOnly_, "Transaction over state is read-only, modification is prohibited");
    tx->HasNoWrites_ = false;
    auto& context = GetTransactionContext(tx, /*forModify*/ true);
    // Do not add to read set in order to avoid conflict in the most common case the result of erase is not used.
    auto&& [itWriteSet, itRecordSet, status] = Lookup(context, key, /*trackRead*/ false);
    UpdateWriteSet(context, itWriteSet, status, key, std::nullopt);
    return status == ELookupStatus::WriteSetInserted || status == ELookupStatus::RecordSetFound;
}

template <class TKey, class TValue>
typename TPersistedState<TKey, TValue>::TRecordPtr TPersistedState<TKey, TValue>::CreateRecord(
    TSequenceId sequenceId,
    TOptKey&& keyLeft,
    TOptKey&& keyRight,
    TOptValue&& value)
{
    if (value.has_value()) {
        THROW_ERROR_EXCEPTION_UNLESS(keyLeft.has_value(), "Bad value record of persisted state: no key present");
        THROW_ERROR_EXCEPTION_IF(keyRight.has_value(), "Bad value record of persisted state: not a point record");
        TInterval<TKey> interval{std::move(keyLeft).value()};
        return New<TRecord>(sequenceId, std::move(interval), std::forward<TOptValue>(value));
    } else {
        TInterval<TKey> interval{std::move(keyLeft), std::move(keyRight)};
        return New<TRecord>(sequenceId, std::move(interval), std::forward<TOptValue>(value));
    }
}

template <class TKey, class TValue>
TPersistedState<TKey, TValue>::TSubscribeCookie TPersistedState<TKey, TValue>::Subscribe(TOnInsert onInsert, TOnErase onErase)
{
    TSubscribeCookie cookie{NextSubscriptionId_++};
    Subscriptions_.emplace(cookie, TSubscription{std::move(onInsert), std::move(onErase)});
    return cookie;
}

template <class TKey, class TValue>
void TPersistedState<TKey, TValue>::Unsubscribe(TSubscribeCookie cookie)
{
    Subscriptions_.erase(cookie);
}

template <class TKey, class TValue>
ssize_t TPersistedState<TKey, TValue>::RecordCount() const
{
    return RecordSet_.size();
}

template <class TKey, class TValue>
void TPersistedState<TKey, TValue>::NotifyInsertSubscribers(const TKey& key)
{
    for (const auto& [id, sub] : Subscriptions_) {
        sub.OnInsert.Underlying()(key);
    }
}

template <class TKey, class TValue>
void TPersistedState<TKey, TValue>::NotifyInsertSubscribers(const TRecordPtr& keyRecord)
{
    YT_ASSERT(keyRecord->Interval.EffectiveLeft().ExtractKey().has_value());
    NotifyInsertSubscribers(keyRecord->Interval.EffectiveLeft().GetKeySure());
}

template <class TKey, class TValue>
void TPersistedState<TKey, TValue>::NotifyEraseSubscribers(const TKey& key)
{
    for (const auto& [id, sub] : Subscriptions_) {
        sub.OnErase.Underlying()(key);
    }
}

template <class TKey, class TValue>
void TPersistedState<TKey, TValue>::NotifyEraseSubscribers(const TRecordPtr& keyRecord)
{
    YT_ASSERT(keyRecord->Interval.EffectiveLeft().ExtractKey().has_value());
    NotifyEraseSubscribers(keyRecord->Interval.EffectiveLeft().GetKeySure());
}

template <class TKey, class TValue>
void TPersistedState<TKey, TValue>::Apply(
    TSequenceId sequenceId,
    TOptKey&& keyLeft,
    TOptKey&& keyRight,
    TOptValue&& value,
    std::deque<TSequenceId>* droppedIds)
{
    THROW_ERROR_EXCEPTION_IF(IsOrphan_, "Persisted state control has been destroyed");
    std::vector<TRecordPtr> insertRecords;
    std::vector<TSequenceId> deleteRecords;

    auto record = CreateRecord(sequenceId, std::move(keyLeft), std::move(keyRight), std::move(value));
    std::vector<typename TRecordSet::node_type> removedKeyNodes;

    // Remove overlapping records.
    auto it = RecordSet_.lower_bound(record->Interval.EffectiveLeft());
    if (it != RecordSet_.begin()) {
        auto prev = std::prev(it);
        if ((*prev)->Interval.EffectiveRight() >= record->Interval.EffectiveLeft()) {
            if (!(*prev)->IsFake()) {
                deleteRecords.emplace_back((*prev)->SequenceId);
            }
            removedKeyNodes.push_back(RecordSet_.extract(prev));
        }
    }
    const auto& effectiveRight = record->Interval.EffectiveRight();
    while (it != RecordSet_.end() && (*it)->Interval.EffectiveLeft() <= effectiveRight) {
        if (!(*it)->IsFake()) {
            deleteRecords.emplace_back((*it)->SequenceId);
        }
        auto removeIt = it;
        ++it;
        removedKeyNodes.push_back(RecordSet_.extract(removeIt));
    }

    // Insert new record.
    auto [inserted, _] = RecordSet_.insert(std::move(record));
    insertRecords.emplace_back(*inserted);

    if (droppedIds != nullptr) {
        std::ranges::copy(deleteRecords, std::back_inserter(*droppedIds));
    }

    // Note that NotifyControl cannot throw with IsOrphan_ == false, so there's no need to undo in case of failure.
    NotifyControl(std::move(insertRecords), deleteRecords);

    for (const auto& node : removedKeyNodes) {
        if (node.value()->Value.has_value()) {
            NotifyEraseSubscribers(node.value());
        }
    }
    if ((*inserted)->Value.has_value()) {
        NotifyInsertSubscribers(*inserted);
    }
}

template <class TKey, class TValue>
void TPersistedState<TKey, TValue>::CheckContents(const TPersistedStateName& name) const
{
    YT_ASSERT(!RecordSet_.empty());
    auto it = RecordSet_.begin();
    if ((*it)->IsFake()) {
        THROW_ERROR_EXCEPTION_UNLESS(std::next(it) == RecordSet_.end(),
            "Fake record in persisted state %Qv must be the one and only record",
            name);
        THROW_ERROR_EXCEPTION_UNLESS((*it)->Interval.EffectiveLeft().Type() == EEndpointType::MinusInf,
            "Fake record in persisted state %Qv must start with minus infinity",
            name);
        THROW_ERROR_EXCEPTION_UNLESS((*it)->Interval.EffectiveRight().Type() == EEndpointType::PlusInf,
            "Fake record in persisted state %Qv must end with plus infinity",
            name);
        return;
    }
    THROW_ERROR_EXCEPTION_UNLESS((*it)->Interval.EffectiveLeft().Type() == EEndpointType::MinusInf,
        "The first record in persisted state %Qv must start with minus infinity",
        name);
    auto lastKey = (*it)->Interval.ExtractRight();
    while (true) {
        ++it;
        if (it == RecordSet_.end()) {
            break;
        }
        YT_ASSERT(lastKey.has_value());
        THROW_ERROR_EXCEPTION_UNLESS((*it)->Value.has_value(),
            "Even record in persisted state %Qv must have a value",
            name);
        THROW_ERROR_EXCEPTION_IF(*lastKey != (*it)->Interval.EffectiveLeft(),
            "There must be no gap before value in persisted state %Qv",
            name);
        ++it;
        THROW_ERROR_EXCEPTION_IF(it == RecordSet_.end(),
            "Unexpected end of records in persisted state %Qv: open interval expected",
            name);
        THROW_ERROR_EXCEPTION_IF((*it)->Value.has_value(),
            "Odd record in persisted state %Qv must not have a value",
            name);
        THROW_ERROR_EXCEPTION_IF(*lastKey != *(*it)->Interval.ExtractLeft(),
            "There must be no gap before open interval in persisted state %Qv",
            name);
        if ((*it)->Interval.EffectiveRight().Type() != EEndpointType::PlusInf) {
            YT_ASSERT((*it)->Interval.EffectiveLeft().Type() == EEndpointType::KeyPlus);
            YT_ASSERT((*it)->Interval.EffectiveRight().Type() == EEndpointType::KeyMinus);
            THROW_ERROR_EXCEPTION_UNLESS((*it)->Interval.EffectiveLeft().GetKeySure() < (*it)->Interval.EffectiveRight().GetKeySure(),
                "Right boundary must be greater than left in persisted state %Qv",
                name);
        }
        lastKey = (*it)->Interval.ExtractRight();
    }
    THROW_ERROR_EXCEPTION_IF(lastKey.has_value(),
        "The last record in persisted state %Qv must end with plus infinity",
        name);
}

template <class TKey, class TValue>
void TPersistedState<TKey, TValue>::PrepareTransaction(
    TPersistedStateTransactionContext<TKey, TValue>& context,
    std::vector<TRecordPtr>& insertRecords,
    std::vector<TSequenceId>& deleteRecords)
{
    THashSet<TRecordPtr> curInsertRecords;
    std::vector<TSequenceId> curDeleteRecords;
    context.ReadSet.clear();

    auto removeSafe = [&] (auto it) {
        auto curtIt = curInsertRecords.find(*it);
        if (curtIt == curInsertRecords.end()) {
            // That's an old record.
            if (!(*it)->IsFake()) {
                curDeleteRecords.push_back((*it)->SequenceId);
            }
            context.UndoDeleted.push_back(RecordSet_.extract(it));
        } else {
            // That's a record that have been just added in this transaction.
            // There's no reason in it, better neither add nor remove it.
            curInsertRecords.erase(curtIt);
            RecordSet_.erase(it);
        }
    };

    auto appendWise = [&] (auto itHint, TRecordPtr&& record) {
        auto it = RecordSet_.insert(itHint, std::move(record));
        curInsertRecords.emplace(*it);
        return it;
    };

    for (const auto& [key, value] : context.WriteSet) {
        YT_ASSERT(!RecordSet_.empty());
        if (value.has_value()) {
            auto it = RecordSet_.lower_bound(key);
            YT_ASSERT(it != RecordSet_.begin());
            bool is_insert = it == RecordSet_.end() || (*it)->Interval.EffectiveLeft() != key;
            if (is_insert) {
                --it;
                YT_ASSERT(!(*it)->Value.has_value());

                TInterval<TKey> leftInterval{(*it)->Interval};
                TInterval<TKey> valueInterval{key};
                TInterval<TKey> rightInterval{key, key};
                rightInterval.SwapRight(leftInterval);

                auto valueIt = appendWise(it, New<TRecord>(FakeSequenceId, std::move(valueInterval), std::move(value.value())));
                removeSafe(it);
                appendWise(valueIt, New<TRecord>(FakeSequenceId, std::move(leftInterval), std::nullopt));
                appendWise(valueIt, New<TRecord>(FakeSequenceId, std::move(rightInterval), std::nullopt));
            } else {
                YT_ASSERT((*it)->Interval.EffectiveRight() == (*it)->Interval.EffectiveLeft());
                YT_ASSERT((*it)->Value.has_value());

                TInterval<TKey> valueInterval{key};

                auto valueIt = std::next(it);
                removeSafe(it);
                appendWise(valueIt, New<TRecord>(FakeSequenceId, std::move(valueInterval), std::move(value.value())));
            }
        } else {
            auto it = RecordSet_.find(key);
            if (it == RecordSet_.end()) {
                continue;
            }
            YT_ASSERT(it != RecordSet_.begin());
            YT_ASSERT(std::next(it) != RecordSet_.end());
            auto leftIt = std::prev(it);
            auto rightIt = std::next(it);

            TInterval<TKey> newInterval{(*leftIt)->Interval.ExtractLeft(), (*rightIt)->Interval.ExtractRight()};

            removeSafe(leftIt);
            appendWise(it, New<TRecord>(FakeSequenceId, std::move(newInterval), std::nullopt));
            removeSafe(it);
            removeSafe(rightIt);
        }
    }
    // Sort insert records in order to make result stable (useful for tests) and also a bit more human readable in the DB table.
    std::vector<TRecordPtr> sortInsertRecords;
    sortInsertRecords.reserve(curInsertRecords.size());
    std::ranges::move(curInsertRecords, std::back_inserter(sortInsertRecords));
    // Note that since records are non-overlapped, the left boundary is unique and thus the sort result is stable.
    std::ranges::sort(sortInsertRecords, [] (const auto& a, const auto& b) {
        return a->Interval.EffectiveLeft() < b->Interval.EffectiveLeft();
    });
    if (insertRecords.empty()) {
        std::swap(sortInsertRecords, insertRecords);
    } else {
        std::ranges::move(sortInsertRecords, std::back_inserter(insertRecords));
    }
    if (deleteRecords.empty()) {
        deleteRecords.reserve(curDeleteRecords.size());
    }
    std::ranges::move(curDeleteRecords, std::back_inserter(deleteRecords));
}

template <class TKey, class TValue>
void TPersistedState<TKey, TValue>::RevertTransaction(TPersistedStateTransactionContext<TKey, TValue>& context)
{
    for (auto& node : context.UndoDeleted) {
        auto& record = node.value();
        // Remove overlapping records.
        auto it = RecordSet_.lower_bound(record->Interval.EffectiveLeft());
        if (it != RecordSet_.begin()) {
            auto prev = std::prev(it);
            if ((*prev)->Interval.EffectiveRight() >= record->Interval.EffectiveLeft()) {
                RecordSet_.erase(prev);
            }
        }
        const auto& effectiveRight = record->Interval.EffectiveRight();
        while (it != RecordSet_.end() && (*it)->Interval.EffectiveLeft() <= effectiveRight) {
            RecordSet_.erase(it++);
        }
        // Insert new record.
        RecordSet_.insert(std::move(node));
    }
    YT_ASSERT(context.ReadSet.empty());
    context.WriteSet.clear();
    context.UndoDeleted.clear();
}

template <class TKey, class TValue>
void TPersistedState<TKey, TValue>::CompleteTransaction(TPersistedStateTransactionContext<TKey, TValue>& context)
{
    for (auto& node : context.UndoDeleted) {
        auto& record = node.value();
        if (record->Value.has_value()) {
            NotifyEraseSubscribers(record);
        }
    }
    for (const auto& [key, value] : context.WriteSet) {
        if (value.has_value()) {
            NotifyInsertSubscribers(key);
        }
    }
    YT_ASSERT(context.ReadSet.empty());
    // Must not clear WriteSet here because it could be needed for conflict detection.
    context.UndoDeleted.clear();
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
TTransactionalPersistedState<TKey, TValue>::TTransactionalPersistedState(
    TPersistedStatePtr<TKey, TValue> state,
    TPersistedStateTransactionPtr transaction)
    : State_(std::move(state))
    , Transaction_(std::move(transaction))
{ }

template <class TKey, class TValue>
void TTransactionalPersistedState<TKey, TValue>::SetState(TPersistedStatePtr<TKey, TValue> state)
{
    State_ = std::move(state);
}

template <class TKey, class TValue>
void TTransactionalPersistedState<TKey, TValue>::SetTransaction(TPersistedStateTransactionPtr transaction)
{
    Transaction_ = std::move(transaction);
}

template <class TKey, class TValue>
bool TTransactionalPersistedState<TKey, TValue>::contains(const TKey& key)
{
    return State_->contains(Transaction_, key);
}

template <class TKey, class TValue>
TValue TTransactionalPersistedState<TKey, TValue>::at(const TKey& key)
{
    return State_->at(Transaction_, key);
}

template <class TKey, class TValue>
const TValue* TTransactionalPersistedState<TKey, TValue>::FindPtr(const TKey& key)
{
    return State_->FindPtr(Transaction_, key);
}

template <class TKey, class TValue>
TTransactionalPersistedState<TKey, TValue>::iterator TTransactionalPersistedState<TKey, TValue>::find(const TKey& key)
{
    return State_->find(Transaction_, key);
}

template <class TKey, class TValue>
TTransactionalPersistedState<TKey, TValue>::iterator TTransactionalPersistedState<TKey, TValue>::begin()
{
    return State_->begin(Transaction_);
}

template <class TKey, class TValue>
TTransactionalPersistedState<TKey, TValue>::iterator TTransactionalPersistedState<TKey, TValue>::end()
{
    return State_->end(Transaction_);
}

template <class TKey, class TValue>
size_t TTransactionalPersistedState<TKey, TValue>::size()
{
    return State_->size(Transaction_);
}

template <class TKey, class TValue>
template <std::convertible_to<TKey> TKeyRef, std::convertible_to<TValue> TValueRef>
std::pair<typename TTransactionalPersistedState<TKey, TValue>::iterator, bool> TTransactionalPersistedState<TKey, TValue>::insert_or_assign(
    TKeyRef&& key,
    TValueRef&& value)
{
    return State_->insert_or_assign(Transaction_, std::forward<TKeyRef>(key), std::forward<TValueRef>(value));
}

template <class TKey, class TValue>
template <class... T>
std::pair<typename TTransactionalPersistedState<TKey, TValue>::iterator, bool> TTransactionalPersistedState<TKey, TValue>::emplace(T&&... t)
{
    return State_->emplace(Transaction_, std::forward<T>(t)...);
}

template <class TKey, class TValue>
size_t TTransactionalPersistedState<TKey, TValue>::erase(const TKey& key)
{
    return State_->erase(Transaction_, key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
