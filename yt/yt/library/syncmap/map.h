#pragma once

#include <util/generic/hash.h>
#include <util/generic/noncopyable.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/hazard_ptr.h>
#include <yt/yt/core/misc/ref_counted.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <atomic>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! A thread-safe insert-only hash map that is optimized for read-mostly workloads.
/*!
 *  When map is not modified, Find() is wait-free.
 *  After modification, next O(n) calls to Find() acquire the lock.
 */
template <
    class TKey,
    class TValue,
    class THash = THash<TKey>,
    class TEqual = TEqualTo<TKey>,
    class TLock = NThreading::TSpinLock
>
class TSyncMap
    : public TNonCopyable
{
public:
    TSyncMap();

    ~TSyncMap();

    template <class TFindKey = TKey>
    TValue* Find(const TFindKey& key);

    template <class TCtor, class TFindKey = TKey>
    std::pair<TValue*, bool> FindOrInsert(const TFindKey& key, TCtor&& ctor);

    //! Flushes dirty map. All keys inserted before this call will be moved to read-only portion of the map.
    //! Designed to facilitate usage of IterateReadOnly.
    void Flush();

    //! IterateReadOnly iterates over read-only portion of the map.
    template <class TFn>
    void IterateReadOnly(TFn&& fn);

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

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

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
