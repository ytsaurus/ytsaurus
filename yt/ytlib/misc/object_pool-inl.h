#ifndef OBJECT_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include object_pool.h"
#endif
#undef OBJECT_POOL_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TObjectPool<T>::TObjectPool()
    : PooledObjectCount(0)
{ }

template <class T>
typename TObjectPool<T>::TValuePtr TObjectPool<T>::Allocate()
{
    T* object = nullptr;
    if (PooledObjects.Dequeue(&object)) {
        AtomicDecrement(PooledObjectCount);
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
    CleanPooledObject(obj);
    PooledObjects.Enqueue(obj);
    // TODO(babenko): make configurable
    if (AtomicIncrement(PooledObjectCount) > 256) {
        T* objToDestroy;
        if (PooledObjects.Dequeue(&objToDestroy)) {
            AtomicDecrement(PooledObjectCount);
            delete objToDestroy;
        }
    }
}

template <class T>
TObjectPool<T>& ObjectPool()
{
    static TObjectPool<T> pool;
    return pool;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
