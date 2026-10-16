#pragma once

#include <util/generic/fwd.h>
#include <util/generic/noncopyable.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <memory>
#include <stddef.h>
#include <vector>

////////////////////////////////////////////////////////////////////////////////

//! An insert-only concurrent hash map.
//!
//! References returned by FindOrInsert and FindPtr are stable for the lifetime of the cache.
//!
//! Optimized for saturation workloads: a small fixed set of keys is inserted once,
//! and the vast majority of subsequent operations are reads.
template <class TKey, class TValue, class THash = THash<TKey>, class TEqual = TEqualTo<TKey>>
class TInsertOnlyConcurrentCache final
    : public TNonCopyable
{
public:
    explicit TInsertOnlyConcurrentCache(THash hash = THash(), TEqual equal = TEqual());

    //! Looks up |key| in the cache. If found, returns a const reference to the stored value.
    //! If not found, calls |initFunctor()| to construct the value, inserts it, and returns a reference.
    //! The returned reference is valid for the lifetime of the cache.
    //! |initFunctor| may be called even if the key is already present during concurrent insertions.
    template <class TInsertionKey, class TInitFunctor>
    const TValue& FindOrInsert(const TInsertionKey& key, TInitFunctor&& initFunctor);

    //! Looks up |key| in the cache. Returns a pointer to the stored value, or nullptr if not found.
    template <class TLookupKey>
    const TValue* FindPtr(const TLookupKey& key) const;

private:
    struct TNode;
    struct TSlot;
    struct TTable;

    template <class TLookupKey>
    const TNode* FindInTable(const TTable& table, const TLookupKey& key) const;

    void InsertIntoTable(TTable& table, TNode* node);

    void MaybeRehash();

private:
    THash Hash_;
    TEqual Equal_;

    std::atomic<TTable*> Snapshot_;                       // Points into SnapshotHolder_; always valid.
    std::vector<std::unique_ptr<TTable>> SnapshotHolder_; // Owns all created tables.

    std::vector<std::unique_ptr<TNode>> NodeStorage_; // All inserted key-value pairs.

    TAdaptiveLock Lock_;
};

////////////////////////////////////////////////////////////////////////////////

#define INSERT_ONLY_CONCURRENT_CACHE_INL_H_
#include "cache-inl.h"
#undef INSERT_ONLY_CONCURRENT_CACHE_INL_H_
