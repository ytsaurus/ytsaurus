#pragma once
#ifndef CONCURRENT_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include concurrent_cache.h"
// For the sake of sane code completion.
#include "concurrent_cache.h"
#endif
#undef CONCURRENT_CACHE_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TConcurrentCache<T>::TLookupTable final
    : public THashTable
{
    using TEnableHazard = void;

    std::atomic<size_t> Size = {0};
    TAtomicPtr<TLookupTable> Next;

    explicit TLookupTable(size_t elementCount)
        : THashTable(elementCount)
    { }
};

template <class T>
void TConcurrentCache<T>::IncrementElementCount(const TIntrusivePtr<TLookupTable>& head)
{
    auto elementCount = ++head->Size;

    if (elementCount < Capacity_ || head != Head_) {
        return;
    }

    // Rotate lookup table.
    auto newHead = New<TLookupTable>(Capacity_);
    newHead->Next = head;

    if (Head_.SwapIfCompare(head, std::move(newHead))) {
        static const auto& Logger = LockFreePtrLogger;
        YT_LOG_DEBUG("Concurrent cache lookup table rotated (ElementCount: %v, LoadFactor: %v)",
            elementCount,
            head->GetLoadFactor());

        // Head_ swapped, remove third lookup table.
        head->Next.Release();
    }
}

template <class T>
TConcurrentCache<T>::TConcurrentCache(size_t capacity)
    : Capacity_(capacity)
    , Head_(New<TLookupTable>(Capacity_))
{ }

template <class T>
TConcurrentCache<T>::~TConcurrentCache()
{
    auto head = Head_.Acquire();

    static const auto& Logger = LockFreePtrLogger;
    YT_LOG_DEBUG("Concurrent cache head statistics (ElementCount: %v, LoadFactor: %v)",
        head->Size.load(),
        head->GetLoadFactor());
}

template <class T>
TConcurrentCache<T>::TInsertAccessor::TInsertAccessor(
    TConcurrentCache* parent,
    TIntrusivePtr<TLookupTable> primary)
    : Parent_(parent)
    , Primary_(std::move(primary))
{
    YT_VERIFY(Parent_);
    YT_VERIFY(Primary_);
}

template <class T>
TConcurrentCache<T>::TInsertAccessor::TInsertAccessor(TInsertAccessor&& other)
    : Parent_(other.Parent_)
    , Primary_(std::move(other.Primary_))
{ }

template <class T>
bool TConcurrentCache<T>::TInsertAccessor::Insert(TFingerprint fingerprint, TValuePtr item)
{
    if (!Primary_->Insert(fingerprint, std::move(item))) {
        return false;
    }

    Parent_->IncrementElementCount(Primary_);
    return true;
}

template <class T>
bool TConcurrentCache<T>::TInsertAccessor::Insert(TValuePtr value)
{
    auto fingerprint = THash<T>()(value.Get());
    return Insert(fingerprint, std::move(value));
}

template <class T>
typename TConcurrentCache<T>::TInsertAccessor TConcurrentCache<T>::GetInsertAccessor()
{
    auto primary = Head_.Acquire();
    return TInsertAccessor(this, std::move(primary));
}

template <class T>
TConcurrentCache<T>::TLookupAccessor::TLookupAccessor(
    TConcurrentCache* parent,
    TIntrusivePtr<TLookupTable> primary,
    TIntrusivePtr<TLookupTable> secondary)
    : TInsertAccessor(parent, std::move(primary))
    , Secondary_(std::move(secondary))
{ }

template <class T>
TConcurrentCache<T>::TLookupAccessor::TLookupAccessor(TLookupAccessor&& other)
    : TInsertAccessor(std::move(other))
    , Secondary_(std::move(other.Secondary_))
{ }

template <class T>
template <class TKey>
TIntrusivePtr<T> TConcurrentCache<T>::TLookupAccessor::Lookup(const TKey& key, bool touch)
{
    auto fingerprint = THash<T>()(key);

    auto item = Primary_->Find(fingerprint, key);

    if (item) {
        return item;
    }

    if (!Secondary_) {
        return nullptr;
    }

    item = Secondary_->Find(fingerprint, key);

    if (item) {
        if (touch && Insert(fingerprint, item)) {
            static const auto& Logger = LockFreePtrLogger;
            YT_LOG_TRACE("Concurrent cache item reinserted");
        }

        return item;
    }

    return nullptr;
}

template <class T>
bool TConcurrentCache<T>::TLookupAccessor::Update(TFingerprint fingerprint, TValuePtr item)
{
    auto updated = Primary_->Update(fingerprint, item);

    if (updated) {
        return true;
    }

    if (Secondary_) {
        updated = Secondary_->Update(fingerprint, std::move(item));
    }

    return updated;
}

template <class T>
bool TConcurrentCache<T>::TLookupAccessor::Update(TValuePtr value)
{
    auto fingerprint = THash<T>()(value.Get());
    return Update(fingerprint, std::move(value));
}

template <class T>
typename TConcurrentCache<T>::TLookupAccessor TConcurrentCache<T>::GetLookupAccessor()
{
    auto primary = Head_.Acquire();
    auto secondary = primary ? primary->Next.Acquire() : nullptr;

    return TLookupAccessor(this, std::move(primary), std::move(secondary));
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
