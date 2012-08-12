#ifndef OBJECT_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include object_pool.h"
#endif
#undef OBJECT_POOL_INL_H_


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TObjectPool<T>::TDeleter::Destroy(T* obj)
{
    TObjectPool<T>::Reclaim(obj);
}

template <class T>
typename TObjectPool<T>::TPtr TObjectPool<T>::Allocate()
{
    T* object;
    if (!PooledObjects.Dequeue(&object)) {
        object = new T();
    }
    return object;
}

template <class T>
void TObjectPool<T>::Reclaim(T* obj)
{
    TObjectPoolCleaner<T>::Clean(obj);
    PooledObjects.Enqueue(obj);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
