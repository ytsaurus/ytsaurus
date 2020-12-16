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
    static constexpr bool EnableHazard = true;

    std::atomic<size_t> Size = 0;
    TAtomicPtr<TLookupTable> Next;

    explicit TLookupTable(size_t elementCount)
        : THashTable(elementCount)
    { }

    bool Insert(TValuePtr item)
    {
        auto fingerprint = THash<T>()(item.Get());
        if (THashTable::Insert(fingerprint, std::move(item))) {
            ++Size;
            return true;
        }
        return false;
    }
};

template <class T>
TIntrusivePtr<typename TConcurrentCache<T>::TLookupTable>
TConcurrentCache<T>::RenewTable(const TIntrusivePtr<TLookupTable>& head)
{
    if (head != Head_) {
        return Head_.Acquire();
    }

    // Rotate lookup table.
    auto newHead = New<TLookupTable>(Capacity_);
    newHead->Next = head;

    if (Head_.SwapIfCompare(head, newHead)) {
        static const auto& Logger = LockFreePtrLogger;
        YT_LOG_DEBUG("Concurrent cache lookup table rotated (LoadFactor: %v)",
            head->Size.load());

        // Head_ swapped, remove third lookup table.
        head->Next.Release();
        return newHead;
    } else {
        return Head_.Acquire();
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
    YT_LOG_DEBUG("Concurrent cache head statistics (ElementCount: %v)",
        head->Size.load());
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
