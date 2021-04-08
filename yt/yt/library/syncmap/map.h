#pragma once

#include <atomic>

#include <util/generic/hash.h>
#include <util/generic/noncopyable.h>
#include <util/system/spinlock.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/hazard_ptr.h>
#include <yt/yt/core/misc/ref_counted.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! TSyncMap is thread-safe insert-only hash map that is optimized for read-mostly workloads.
//!
//! When map is not modified, Find() is wait-free.
//!
//! After modification, next O(n) calls to Find() acquire global spinlock.
template <class TKey, class TValue, class THash = THash<TKey>, class TEqual = TEqualTo<TKey>>
class TSyncMap
    : public TNonCopyable
{
public:
    TSyncMap();

    ~TSyncMap();

    template <class TFindKey = TKey>
    TValue* Find(const TFindKey& key);

    template <class TCtor, class TFindKey = TKey>
    std::pair<TValue*, bool> FindOrInsert(const TFindKey& key, const TCtor& ctor);

    //! RangeReadOnly iterates over read-only portion of the map.
    template <class TFn>
    void IterateReadOnly(const TFn& fn);

private:
    struct TEntry final
    {
        explicit TEntry(TValue value)
            : Value(std::move(value))
        { }

        TValue Value;
    };

    struct TMap final
        : public THashMap<TKey, TIntrusivePtr<TEntry>, THash, TEqual>
    { };

    struct TSnapshot
    {
        static constexpr bool EnableHazard = true;

        TIntrusivePtr<TMap> Map = New<TMap>();
        bool Dirty = false;
    };

    std::atomic<TSnapshot*> Snapshot_ = nullptr;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock_);

    TIntrusivePtr<TMap> DirtyMap_;

    size_t Misses_ = 0;

    void OnMiss();

    THazardPtr<TSnapshot> AcquireSnapshot();
    void UpdateSnapshot(TIntrusivePtr<TMap> map, bool dirty);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define MAP_INL_H_
#include "map-inl.h"
#undef MAP_INL_H_
