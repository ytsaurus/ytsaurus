#pragma once

#include "common.h"

#include <util/thread/lfqueue.h>

#include <util/generic/singleton.h>

#include <core/profiling/public.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A pool for reusable objects.
/*
 * Instances are tracked via shared pointers with a special deleter
 * that returns spare instances back to the pool.
 *
 * Both the pool and the references are thread-safe.
 *
 */
template <class T>
class TObjectPool
{
public:
    typedef std::shared_ptr<T> TValuePtr;

    TObjectPool();

    //! Either creates a fresh instance or returns a pooled one.
    TValuePtr Allocate();

    //! Calls #TPooledObjectTraits::Clean and returns the instance back into the pool.
    void Reclaim(T* obj);

private:
    struct THeader
    {
        NProfiling::TCpuInstant ExpireInstant;
    };

    static_assert(sizeof(THeader) % 8 == 0, "THeader must be padded to ensure proper alignment.");

    TLockFreeQueue<T*> PooledObjects_;
    std::atomic<int> PoolSize_;


    T* AllocateInstance();
    void FreeInstance(T* obj);

    THeader* GetHeader(T* obj);
    bool IsExpired(const THeader* header);

};

template <class T>
TObjectPool<T>& ObjectPool();

////////////////////////////////////////////////////////////////////////////////

//! Provides various traits for pooled objects of type |T|.
/*!
 * |Clean| method is called before an object is put into the pool.
 * 
 * |GetMaxPoolSize| method is called to determine the maximum number of
 * objects allowed to be pooled.
 *
 * |GetMaxLifetime| method is called to determine the maximum amount of
 * time a pooled instance is allowed to live (plus a random duration not
 * in the range from 0 to |GetMaxLifetimeSplay|).
 */
template <class T, class = void>
struct TPooledObjectTraits
{ };

//! Basic version of traits. Others may consider inheriting from it.
struct TPooledObjectTraitsBase
{
    template <class T>
    static void Clean(T*)
    { }

    static int GetMaxPoolSize()
    {
        return 256;
    }

    static TDuration GetMaxLifetime()
    {
        return TDuration::Seconds(60);
    }

    static TDuration GetMaxLifetimeSplay()
    {
        return TDuration::Seconds(60);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define OBJECT_POOL_INL_H_
#include "object_pool-inl.h"
#undef OBJECT_POOL_INL_H_
