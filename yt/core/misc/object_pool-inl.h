#pragma once
#ifndef OBJECT_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include object_pool.h"
#endif

#include "mpl.h"
#include "ref_counted_tracker.h"

#include <yt/core/utilex/random.h>

#include <yt/core/profiling/timing.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TObjectPool<T>::~TObjectPool()
{
    T* obj;
    while (PooledObjects_.Dequeue(&obj)) {
        FreeInstance(obj);
    }
}

template <class T>
auto TObjectPool<T>::Allocate() -> TObjectPtr
{
    auto now = NProfiling::GetCpuInstant();

    T* obj = nullptr;
    while (PooledObjects_.Dequeue(&obj)) {
        --PoolSize_;

        auto* header = GetHeader(obj);
        if (!IsExpired(header, now)) {
            break;
        }

        FreeInstance(obj);
        obj = nullptr;
    }

    if (!obj) {
        obj = AllocateInstance(now);
    }

    return TObjectPtr(obj, [] (T* obj) {
        ObjectPool<T>().Reclaim(obj);
    });
}

template <class T>
void TObjectPool<T>::Reclaim(T* obj)
{
    auto* header = GetHeader(obj);
    auto now = NProfiling::GetCpuInstant();
    if (IsExpired(header, now)) {
        FreeInstance(obj);
        return;
    }

    TPooledObjectTraits<T>::Clean(obj);

    while (true) {
        auto poolSize = PoolSize_.load();
        if (poolSize >= TPooledObjectTraits<T>::GetMaxPoolSize()) {
            FreeInstance(obj);
            break;
        } else if (PoolSize_.compare_exchange_strong(poolSize, poolSize + 1)){
            PooledObjects_.Enqueue(obj);
            break;
        }
    }

    if (PoolSize_ > TPooledObjectTraits<T>::GetMaxPoolSize()) {
        T* objToDestroy;
        if (PooledObjects_.Dequeue(&objToDestroy)) {
            --PoolSize_;
            FreeInstance(objToDestroy);
        }
    }
}

template <class T>
T* TObjectPool<T>::AllocateInstance(NProfiling::TCpuInstant now)
{
    auto cookie = GetRefCountedTypeCookie<T>();
    TRefCountedTracker::Get()->Allocate(cookie, sizeof (T));
    char* buffer = new char[sizeof (THeader) + sizeof (T)];
    auto* header = reinterpret_cast<THeader*>(buffer);
    auto* obj = reinterpret_cast<T*>(header + 1);
    new (obj) T();
    header->ExpireInstant =
        now +
        NProfiling::DurationToCpuDuration(
            TPooledObjectTraits<T>::GetMaxLifetime() +
            RandomDuration(TPooledObjectTraits<T>::GetMaxLifetimeSplay()));
    return obj;
}

template <class T>
void TObjectPool<T>::FreeInstance(T* obj)
{
    auto cookie = GetRefCountedTypeCookie<T>();
    TRefCountedTracker::Get()->Free(cookie, sizeof (T));
    obj->~T();
    auto* buffer = reinterpret_cast<char*>(obj) - sizeof (THeader);
    delete[] buffer;
}

template <class T>
typename TObjectPool<T>::THeader* TObjectPool<T>::GetHeader(T* obj)
{
    return reinterpret_cast<THeader*>(obj) - 1;
}

template <class T>
bool TObjectPool<T>::IsExpired(const THeader* header, NProfiling::TCpuInstant now)
{
    return now > header->ExpireInstant;
}

template <class T>
TObjectPool<T>& ObjectPool()
{
    return *Singleton<TObjectPool<T>>();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TPooledObjectTraits<
    T,
    typename NMpl::TEnableIf<
        NMpl::TIsConvertible<T&, ::google::protobuf::MessageLite&>
    >::TType
>
    : public TPooledObjectTraitsBase
{
    static void Clean(T* message)
    {
        message->Clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
