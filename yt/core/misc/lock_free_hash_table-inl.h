#pragma once
#ifndef LOCK_FREE_HASH_TABLE_INL_H_
#error "Direct inclusion of this file is not allowed, include lock_free_hash_table.h"
// For the sake of sane code completion.
#include "lock_free_hash_table.h"
#endif
#undef LOCK_FREE_HASH_TABLE_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class TAlloc>
TLockFreeHashTable<T, TAlloc>::TLockFreeHashTable(size_t maxElementCount)
    : Size_(maxElementCount * HashTableExpansionFactor)
    , HashTable_(new std::atomic<TEntry>[Size_]())
{ }

template <class T, class TAlloc>
TLockFreeHashTable<T, TAlloc>::~TLockFreeHashTable()
{
    for (size_t index = 0; index < Size_; ++index) {
        auto tableEntry = HashTable_[index].load(std::memory_order_relaxed);
        auto stamp = StampFromEntry(tableEntry);
        if (stamp != 0) {
            ScheduleObjectDeletion(ValueFromEntry(tableEntry), [] (void* ptr) {
                ReleaseRef<TAlloc>(static_cast<T*>(ptr));
            });
        }
    }
}

template <class T, class TAlloc>
size_t TLockFreeHashTable<T, TAlloc>::GetByteSize() const
{
    return sizeof(std::atomic<TEntry>) * Size_;
}

template <class T, class TAlloc>
template <class TCallback>
void TLockFreeHashTable<T, TAlloc>::ForEach(TCallback callback)
{
    for (size_t index = 0; index < Size_; ++index) {
        auto tableEntry = HashTable_[index].load(std::memory_order_relaxed);
        auto stamp = StampFromEntry(tableEntry);
        if (stamp != 0) {
            callback(ValueFromEntry(tableEntry));
        }
    }
}

template <class T, class TAlloc>
size_t TLockFreeHashTable<T, TAlloc>::GetLoadFactor()
{
    size_t result = 0;
    for (size_t index = 0; index < Size_; ++index) {
        auto tableEntry = HashTable_[index].load(std::memory_order_relaxed);
        auto stamp = StampFromEntry(tableEntry);
        result += stamp != 0 ? 1 : 0;
    }

    return result;
}

template <class T, class TAlloc>
bool TLockFreeHashTable<T, TAlloc>::Update(TFingerprint fingerprint, TValuePtr value)
{
    auto index = IndexFromFingerprint(fingerprint) % Size_;
    auto stamp = StampFromFingerprint(fingerprint);

    auto entry = MakeEntry(stamp, value.Get());

    for (size_t probeCount = Size_; probeCount != 0;) {
        auto tableEntry = HashTable_[index].load(std::memory_order_relaxed);
        auto tableStamp = StampFromEntry(tableEntry);

        if (tableStamp == 0) {
            break;
        }

        if (tableStamp == stamp) {
            // This hazard ptr protects from Unref. We do not want to change ref count so frequently.
            auto item = THazardPtr<T>::Acquire([&] {
                return ValueFromEntry(HashTable_[index].load(std::memory_order_relaxed));
            }, ValueFromEntry(tableEntry));

            if (TEqualTo<T>()(item.Get(), value.Get())) {
                // Exchange allows to use this function concurrently.
                auto oldElement = HashTable_[index].exchange(entry);
                value.Release();
                item.Reset();

                ScheduleObjectDeletion(ValueFromEntry(oldElement), [] (void* ptr) {
                    ReleaseRef<TAlloc>(static_cast<T*>(ptr));
                });

                return true;
            }
        }

        ++index;
        if (index == Size_) {
            index = 0;
        }
        --probeCount;
    }

    return false;
}

template <class T, class TAlloc>
bool TLockFreeHashTable<T, TAlloc>::Insert(TFingerprint fingerprint, TValuePtr value)
{
    auto index = IndexFromFingerprint(fingerprint) % Size_;
    auto stamp = StampFromFingerprint(fingerprint);

    auto entry = MakeEntry(stamp, value.Get());

    for (size_t probeCount = Size_; probeCount != 0;) {
        auto tableEntry = HashTable_[index].load(std::memory_order_relaxed);
        auto tableStamp = StampFromEntry(tableEntry);

        if (tableStamp == 0) {
            auto success = HashTable_[index].compare_exchange_strong(
                tableEntry,
                entry,
                std::memory_order_release,
                std::memory_order_relaxed);
            if (success) {
                value.Release();
                return true;
            }
        }

        // This hazard ptr protects from Unref. We do not want to change ref count so frequently.
        auto item = THazardPtr<T>::Acquire([&] {
            return ValueFromEntry(HashTable_[index].load(std::memory_order_relaxed));
        }, ValueFromEntry(tableEntry));

        if (TEqualTo<T>()(item.Get(), value.Get())) {
            return false;
        }

        ++index;
        if (index == Size_) {
            index = 0;
        }
        --probeCount;
    }

    return false;
}

template <class T, class TAlloc>
template <class TKey>
TRefCountedPtr<T, TAlloc> TLockFreeHashTable<T, TAlloc>::Find(TFingerprint fingerprint, const TKey& key)
{
    auto index = IndexFromFingerprint(fingerprint) % Size_;
    auto stamp = StampFromFingerprint(fingerprint);

    for (size_t probeCount = Size_; probeCount != 0;) {
        auto tableEntry = HashTable_[index].load(std::memory_order_relaxed);
        auto tableStamp = StampFromEntry(tableEntry);

        if (tableStamp == 0) {
            break;
        }

        if (tableStamp == stamp) {
            // This hazard ptr protects from Unref. We do not want to change ref count so frequently.
            // TRefCountedPtr::AcquireUnchecked could be used outside this function.

            auto item = THazardPtr<T>::Acquire([&] {
                return ValueFromEntry(HashTable_[index].load(std::memory_order_relaxed));
            }, ValueFromEntry(tableEntry));

            if (TEqualTo<T>()(item.Get(), key)) {
                return TValuePtr(item.Get());
            }
        }

        ++index;
        if (index == Size_) {
            index = 0;
        }
        --probeCount;
    }

    return nullptr;
}

template <class T, class TAlloc>
typename TLockFreeHashTable<T, TAlloc>::TStamp
    TLockFreeHashTable<T, TAlloc>::StampFromEntry(TEntry entry)
{
    return entry >> ValueLog;
}

template <class T, class TAlloc>
T* TLockFreeHashTable<T, TAlloc>::ValueFromEntry(TEntry entry)
{
    return reinterpret_cast<T*>(entry & ((1ULL << ValueLog) - 1));
}

template <class T, class TAlloc>
typename TLockFreeHashTable<T, TAlloc>::TEntry
    TLockFreeHashTable<T, TAlloc>::MakeEntry(TStamp stamp, T* value)
{
    YT_ASSERT(stamp != 0);
    YT_ASSERT(StampFromEntry(reinterpret_cast<TEntry>(value)) == 0);
    return (static_cast<TEntry>(stamp) << ValueLog) | reinterpret_cast<TEntry>(value);
}

template <class T, class TAlloc>
size_t TLockFreeHashTable<T, TAlloc>::IndexFromFingerprint(TFingerprint fingerprint)
{
    // TODO(lukyan): Use higher bits of fingerprint. Lower are used by stamp.
    return fingerprint;
}

template <class T, class TAlloc>
typename TLockFreeHashTable<T, TAlloc>::TStamp
    TLockFreeHashTable<T, TAlloc>::StampFromFingerprint(TFingerprint fingerprint)
{
    return (fingerprint << 1) | 1ULL;
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT
