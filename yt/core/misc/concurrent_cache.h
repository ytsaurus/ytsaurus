#pragma once

#include "public.h"
#include "atomic_ptr.h"
#include "lock_free_hash_table.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TConcurrentCache
{
private:
    using THashTable = TLockFreeHashTable<T>;

    struct TLookupTable;

    void IncrementElementCount(const TIntrusivePtr<TLookupTable>& head);

public:
    using TValuePtr = TIntrusivePtr<T>;

    explicit TConcurrentCache(size_t maxElementCount);

    ~TConcurrentCache();

    class TInsertAccessor
    {
    public:
        TInsertAccessor(
            TConcurrentCache* parent,
            TIntrusivePtr<TLookupTable> primary);

        TInsertAccessor(TInsertAccessor&& other);

        // TODO(lukyan): Return inserted value (existing, new) or nullptr.
        bool Insert(TFingerprint fingerprint, TValuePtr item);

        bool Insert(TValuePtr value);
    protected:
        TConcurrentCache* const Parent_;
        TIntrusivePtr<TLookupTable> Primary_;
    };

    TInsertAccessor GetInsertAccessor();

    class TLookupAccessor
        : public TInsertAccessor
    {
    public:
        using TInsertAccessor::Insert;

        TLookupAccessor(
            TConcurrentCache* parent,
            TIntrusivePtr<TLookupTable> primary,
            TIntrusivePtr<TLookupTable> secondary);

        TLookupAccessor(TLookupAccessor&& other);

        template <class TKey>
        TIntrusivePtr<T> Lookup(const TKey& key, bool touch = false);

        bool Update(TFingerprint fingerprint, TValuePtr item);

        bool Update(TValuePtr value);
    private:
        using TInsertAccessor::Parent_;
        using TInsertAccessor::Primary_;

        // TODO(lukyan): Acquire secondary lazily
        TIntrusivePtr<TLookupTable> Secondary_;

    };

    TLookupAccessor GetLookupAccessor();

public:
    const size_t Capacity_;
    TAtomicPtr<TLookupTable> Head_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CONCURRENT_CACHE_INL_H_
#include "concurrent_cache-inl.h"
#undef CONCURRENT_CACHE_INL_H_
