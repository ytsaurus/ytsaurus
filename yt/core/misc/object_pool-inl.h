#pragma once
#ifndef OBJECT_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include object_pool.h"
#endif

#include "mpl.h"

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
    T* obj = nullptr;
    if (PooledObjects_.Dequeue(&obj)) {
        --PoolSize_;
    }

    if (!obj) {
        obj = TPooledObjectTraits<T>::Allocate();
    }

    return TObjectPtr(obj, [] (T* obj) {
        ObjectPool<T>().Reclaim(obj);
    });
}

template <class T>
void TObjectPool<T>::Reclaim(T* obj)
{
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
void TObjectPool<T>:: Release(size_t count)
{
    T* obj;
    while (PooledObjects_.Dequeue(&obj) && count) {
        --PoolSize_;
        FreeInstance(obj);
        --count;
    }
}

template <class T>
void TObjectPool<T>::FreeInstance(T* obj)
{
    delete obj;
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
    : public TPooledObjectTraitsBase<T>
{
    static void Clean(T* message)
    {
        message->Clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
