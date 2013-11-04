#pragma once

#include "common.h"

#include <util/thread/lfqueue.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A pool for reusable objects.
/*
 * Instances are tracked via shared pointers with a special deleter
 * that returns spare instances back to the pool.
 *
 * Types capable of pooling must provide |CleanPooledObject| method.
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

    //! Calls #CleanPooledObject and returns the instance back into the pool.
    void Reclaim(T* obj);

private:
    TLockFreeQueue<T*> PooledObjects;
    TAtomic PooledObjectCount;

};

template <class T>
TObjectPool<T>& ObjectPool();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define OBJECT_POOL_INL_H_
#include "object_pool-inl.h"
#undef OBJECT_POOL_INL_H_

