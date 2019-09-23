#pragma once
#ifndef CONCURRENT_CACHE_INL_H_
#error "Direct inclusion of this file is not allowed, include concurrent_cache.h"
// For the sake of sane code completion.
#include "concurrent_cache.h"
#endif
#undef CONCURRENT_CACHE_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class TAlloc>
struct TConcurrentCache<T, TAlloc>::TLookupTable
    : public THashTable
{
    std::atomic<size_t> Size = {0};
    TAtomicPtr<TLookupTable> Next;

    explicit TLookupTable(size_t elementCount)
        : THashTable(elementCount)
    { }
};

template <class T, class TAlloc>
void TConcurrentCache<T, TAlloc>::IncrementElementCount(const TRefCountedPtr<TLookupTable>& head)
{
    auto elementCount = ++head->Size;

    if (elementCount < Capacity_ || head != Head_) {
        return;
    }

    // Rotate lookup table.
    auto newHead = CreateObject<TLookupTable>(Capacity_);
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

template <class T, class TAlloc>
TConcurrentCache<T, TAlloc>::TConcurrentCache(size_t capacity)
    : Capacity_(capacity)
    , Head_(CreateObject<TLookupTable>(Capacity_))
{ }

template <class T, class TAlloc>
TConcurrentCache<T, TAlloc>::~TConcurrentCache()
{
    auto head = Head_.Acquire();

    static const auto& Logger = LockFreePtrLogger;
    YT_LOG_DEBUG("Concurrent cache head statistics (ElementCount: %v, LoadFactor: %v)",
        head->Size.load(),
        head->GetLoadFactor());
}

template <class T, class TAlloc>
TConcurrentCache<T, TAlloc>::TInsertAccessor::TInsertAccessor(
    TConcurrentCache* parent,
    TRefCountedPtr<TLookupTable> primary)
    : Parent_(parent)
    , Primary_(std::move(primary))
{
    YT_VERIFY(Parent_);
    YT_VERIFY(Primary_);
}

template <class T, class TAlloc>
TConcurrentCache<T, TAlloc>::TInsertAccessor::TInsertAccessor(TInsertAccessor&& other)
    : Parent_(other.Parent_)
    , Primary_(std::move(other.Primary_))
{ }

template <class T, class TAlloc>
bool TConcurrentCache<T, TAlloc>::TInsertAccessor::Insert(TFingerprint fingerprint, TValuePtr item)
{
    if (!Primary_->Insert(fingerprint, std::move(item))) {
        return false;
    }

    Parent_->IncrementElementCount(Primary_);
    return true;
}

template <class T, class TAlloc>
bool TConcurrentCache<T, TAlloc>::TInsertAccessor::Insert(TValuePtr value)
{
    auto fingerprint = THash<T>()(value.Get());
    return Insert(fingerprint, std::move(value));
}

template <class T, class TAlloc>
typename TConcurrentCache<T, TAlloc>::TInsertAccessor TConcurrentCache<T, TAlloc>::GetInsertAccessor()
{
    auto primary = Head_.Acquire();
    return TInsertAccessor(this, std::move(primary));
}

template <class T, class TAlloc>
TConcurrentCache<T, TAlloc>::TLookupAccessor::TLookupAccessor(
    TConcurrentCache* parent,
    TRefCountedPtr<TLookupTable> primary,
    TRefCountedPtr<TLookupTable> secondary)
    : TInsertAccessor(parent, std::move(primary))
    , Secondary_(std::move(secondary))
{ }

template <class T, class TAlloc>
TConcurrentCache<T, TAlloc>::TLookupAccessor::TLookupAccessor(TLookupAccessor&& other)
    : TInsertAccessor(std::move(other))
    , Secondary_(std::move(other.Secondary_))
{ }

template <class T, class TAlloc>
template <class TKey>
TRefCountedPtr<T, TAlloc> TConcurrentCache<T, TAlloc>::TLookupAccessor::Lookup(const TKey& key, bool touch)
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

template <class T, class TAlloc>
bool TConcurrentCache<T, TAlloc>::TLookupAccessor::Update(TFingerprint fingerprint, TValuePtr item)
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

template <class T, class TAlloc>
bool TConcurrentCache<T, TAlloc>::TLookupAccessor::Update(TValuePtr value)
{
    auto fingerprint = THash<T>()(value.Get());
    return Update(fingerprint, std::move(value));
}

template <class T, class TAlloc>
typename TConcurrentCache<T, TAlloc>::TLookupAccessor TConcurrentCache<T, TAlloc>::GetLookupAccessor()
{
    auto primary = Head_.Acquire();
    auto secondary = primary ? primary->Next.Acquire() : nullptr;

    return TLookupAccessor(this, std::move(primary), std::move(secondary));
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
