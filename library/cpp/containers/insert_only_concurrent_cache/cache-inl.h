#pragma once

#ifndef INSERT_ONLY_CONCURRENT_CACHE_INL_H_
    #error "Direct inclusion of this file is not allowed, include cache.h"
    // For the sake of sane code completion.
    #include "cache.h"
#endif

#include <util/generic/xrange.h>
#include <util/system/guard.h>
#include <util/system/yassert.h>

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash, class TEqual>
struct TInsertOnlyConcurrentCache<TKey, TValue, THash, TEqual>::TNode
{
    TKey Key;
    TValue Value;
};

template <class TKey, class TValue, class THash, class TEqual>
struct TInsertOnlyConcurrentCache<TKey, TValue, THash, TEqual>::TSlot
{
    std::atomic<TNode*> Node{nullptr};
};

template <class TKey, class TValue, class THash, class TEqual>
struct TInsertOnlyConcurrentCache<TKey, TValue, THash, TEqual>::TTable
{
    size_t Capacity;                // Power of 2.
    size_t Mask;                    // Bitmask for fast index wrapping: index & Mask instead of index % Capacity.
    std::unique_ptr<TSlot[]> Slots; // Open addressing slots.
    size_t Size = 0;

public:
    explicit TTable(size_t capacity)
        : Capacity(capacity)
        , Mask(capacity - 1)
        , Slots(std::make_unique<TSlot[]>(capacity))
    {
        Y_DEBUG_ABORT_UNLESS(capacity > 0 && (capacity & (capacity - 1)) == 0, "Capacity must be a power of 2");
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash, class TEqual>
template <class TLookupKey>
const typename TInsertOnlyConcurrentCache<TKey, TValue, THash, TEqual>::TNode*
TInsertOnlyConcurrentCache<TKey, TValue, THash, TEqual>::FindInTable(
    const TTable& table,
    const TLookupKey& key) const
{
    size_t index = Hash_(key) & table.Mask;
    for (size_t i : xrange<size_t>(index, table.Capacity)) {
        const auto* node = table.Slots[i].Node.load(std::memory_order_acquire);
        if (!node) {
            return nullptr;
        }
        if (Equal_(node->Key, key)) {
            return node;
        }
    }
    for (size_t i : xrange<size_t>(0, index)) {
        const auto* node = table.Slots[i].Node.load(std::memory_order_acquire);
        if (!node) {
            return nullptr;
        }
        if (Equal_(node->Key, key)) {
            return node;
        }
    }
    return nullptr;
}

template <class TKey, class TValue, class THash, class TEqual>
void TInsertOnlyConcurrentCache<TKey, TValue, THash, TEqual>::InsertIntoTable(
    TTable& table,
    TNode* node)
{
    Y_DEBUG_ABORT_UNLESS(table.Size < table.Capacity);
    size_t index = Hash_(node->Key) & table.Mask;
    for (size_t i : xrange<size_t>(index, table.Capacity)) {
        if (!table.Slots[i].Node.load(std::memory_order_relaxed)) {
            table.Slots[i].Node.store(node, std::memory_order_release);
            ++table.Size;
            return;
        }
    }
    for (size_t i : xrange<size_t>(0, index)) {
        if (!table.Slots[i].Node.load(std::memory_order_relaxed)) {
            table.Slots[i].Node.store(node, std::memory_order_release);
            ++table.Size;
            return;
        }
    }
    Y_ABORT("Table is corrupted: no empty slot found despite size < capacity");
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash, class TEqual>
TInsertOnlyConcurrentCache<TKey, TValue, THash, TEqual>::TInsertOnlyConcurrentCache(
    THash hash,
    TEqual equal)
    : Hash_(std::move(hash))
    , Equal_(std::move(equal))
{
    SnapshotHolder_.push_back(std::make_unique<TTable>(8));
    Snapshot_.store(SnapshotHolder_.back().get(), std::memory_order_release);
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, class THash, class TEqual>
template <class TLookupKey>
const TValue* TInsertOnlyConcurrentCache<TKey, TValue, THash, TEqual>::FindPtr(
    const TLookupKey& key) const
{
    const auto* node = FindInTable(*Snapshot_.load(std::memory_order_acquire), key);
    return node ? &node->Value : nullptr;
}

template <class TKey, class TValue, class THash, class TEqual>
template <class TInsertionKey, class TInitFunctor>
const TValue& TInsertOnlyConcurrentCache<TKey, TValue, THash, TEqual>::FindOrInsert(
    const TInsertionKey& key,
    TInitFunctor&& initFunctor)
{
    if (const auto* node = FindInTable(*Snapshot_.load(std::memory_order_acquire), key)) {
        return node->Value;
    }

    auto nodeOwner = std::unique_ptr<TNode>(new TNode{.Key = TKey(key), .Value = initFunctor()});
    auto* node = nodeOwner.get();

    TGuard guard(Lock_);

    // Re-check under the lock — another thread may have inserted the key.
    if (const auto* existing = FindInTable(*Snapshot_.load(std::memory_order_acquire), key)) {
        return existing->Value;
    }

    MaybeRehash();

    InsertIntoTable(*Snapshot_.load(std::memory_order_relaxed), node);

    NodeStorage_.push_back(std::move(nodeOwner));
    return node->Value;
}

template <class TKey, class TValue, class THash, class TEqual>
void TInsertOnlyConcurrentCache<TKey, TValue, THash, TEqual>::MaybeRehash()
{
    // Snapshot_ is filled in the constructor or previous MaybeRehash calls.
    auto* table = Snapshot_.load(std::memory_order_acquire);

    size_t currentSize = table->Size;
    size_t currentCapacity = table->Capacity;

    // Grow if fill ratio > 25% (i.e. (size + 1) / capacity > 1/4).
    if ((currentSize + 1) * 4 <= currentCapacity) {
        return;
    }
    size_t newCapacity = currentCapacity * 2;

    auto newTable = std::make_unique<TTable>(newCapacity);

    // Copy all existing node pointers into the new table.
    for (size_t i = 0; i < currentCapacity; ++i) {
        if (auto* node = table->Slots[i].Node.load(std::memory_order_relaxed)) {
            InsertIntoTable(*newTable, node);
        }
    }

    // Publish the new table.
    Snapshot_.store(newTable.get(), std::memory_order_release);
    SnapshotHolder_.push_back(std::move(newTable));
}

////////////////////////////////////////////////////////////////////////////////
