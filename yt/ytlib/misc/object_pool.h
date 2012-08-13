#pragma once

#include "common.h"

#include <util/thread/lfstack.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A pool for reusable objects.
/*
 * Instances are tracked via shared pointers with a special deleter
 * that returns spare instances back to the pool.
 * 
 * Types capable of pooling must specialize #TObjectPoolCleaner
 * and provide |Clean| method.
 * 
 * Both the pool and the references are thread-safe.
 * 
 */
template <class T>
class TObjectPool
{
private:
    struct TDeleter
    {
        static void Destroy(T* obj);
    };

public:
    typedef TSharedPtr<T, TAtomicCounter, TDeleter> TValuePtr;

    //! Either creates a fresh instance or returns a pooled one.
    TValuePtr Allocate();

    //! Calls #CleanPooledObject and returns the instance back into the pool.
    void Reclaim(T* obj);

private:
    TLockFreeStack<T*> PooledObjects;

};

template <class T>
TObjectPool<T>& ObjectPool();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define OBJECT_POOL_INL_H_
#include "object_pool-inl.h"
#undef OBJECT_POOL_INL_H_

