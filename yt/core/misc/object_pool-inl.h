#ifndef OBJECT_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include object_pool.h"
#endif
#undef OBJECT_POOL_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TObjectPool<T>::TObjectPool()
    : PoolSize_(0)
{ }

template <class T>
typename TObjectPool<T>::TValuePtr TObjectPool<T>::Allocate()
{
    T* object = nullptr;
    if (PooledObjects_.Dequeue(&object)) {
        AtomicDecrement(PoolSize_);
    } else {
        object = new T();
    }
    return TValuePtr(object, [] (T* object) {
        ObjectPool<T>().Reclaim(object);
    });
}

template <class T>
void TObjectPool<T>::Reclaim(T* obj)
{
    TPooledObjectTraits<T>::Clean(obj);
    PooledObjects_.Enqueue(obj);
    if (AtomicIncrement(PoolSize_) > TPooledObjectTraits<T>::GetMaxPoolSize()) {
        T* objToDestroy;
        if (PooledObjects_.Dequeue(&objToDestroy)) {
            AtomicDecrement(PoolSize_);
            delete objToDestroy;
        }
    }
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

} // namespace NYT
