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
    PooledObjects_.Enqueue(obj);

    if (++PoolSize_ > TPooledObjectTraits<T>::GetMaxPoolSize()) {
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
    static auto* cookie = ::NYT::NDetail::GetRefCountedTrackerCookie<T>();
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
    static auto* cookie = ::NYT::NDetail::GetRefCountedTrackerCookie<T>();
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
#ifdef _MSC_VER
    // XXX(babenko): MSVC (upto version 2013) does not support thread-safe static locals init :(
    static TAtomic lock = 0;
    static TAtomic pool = 0;
    while (!pool) {
        if (AtomicCas(&lock, 1, 0)) {
            if (!pool) {
                AtomicSet(pool, reinterpret_cast<intptr_t>(new TObjectPool<T>()));
            }
            AtomicSet(lock, 0);
        } else {
            SpinLockPause();
        }
    }
    return *reinterpret_cast<TObjectPool<T>*>(pool);
#else
    static TObjectPool<T> pool;
    return pool;
#endif
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
    static void Clean(::google::protobuf::MessageLite* obj)
    {
        obj->Clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
