#ifndef OBJECT_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include object_pool.h"
#endif
#undef OBJECT_POOL_INL_H_

#include "mpl.h"
#include "ref_counted_tracker.h"

#include <util/random/random.h>

#include <core/profiling/timing.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TObjectPool<T>::TObjectPool()
    : PoolSize_(0)
{ }

template <class T>
typename TObjectPool<T>::TValuePtr TObjectPool<T>::Allocate()
{
    T* obj = nullptr;
    while (PooledObjects_.Dequeue(&obj)) {
        --PoolSize_;

        auto* header = GetHeader(obj);
        if (!IsExpired(header))
            break;

        FreeInstance(obj);
        obj = nullptr;
    }

    if (!obj) {
        obj = AllocateInstance();
    }
    
    return TValuePtr(obj, [] (T* obj) {
        ObjectPool<T>().Reclaim(obj);
    });
}

template <class T>
void TObjectPool<T>::Reclaim(T* obj)
{
    auto* header = GetHeader(obj);
    if (IsExpired(header)) {
        FreeInstance(obj);
        return;
    }

    TPooledObjectTraits<T>::Clean(obj);
    if (PoolSize_ < TPooledObjectTraits<T>::GetMaxPoolSize()) {
        PooledObjects_.Enqueue(obj);
        ++PoolSize_;
    } else {
        FreeInstance(obj);
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
T* TObjectPool<T>::AllocateInstance()
{
    auto cookie = GetRefCountedTypeCookie<T>();
    TRefCountedTracker::Get()->Allocate(cookie, sizeof (T));
    char* buffer = new char[sizeof (THeader) + sizeof (T)];
    auto* header = reinterpret_cast<THeader*>(buffer);
    auto* obj = reinterpret_cast<T*>(header + 1);
    new (obj) T();
    header->ExpireInstant =
        NProfiling::GetCpuInstant() +
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
bool TObjectPool<T>::IsExpired(const THeader* header)
{
    return NProfiling::GetCpuInstant() > header->ExpireInstant;
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
