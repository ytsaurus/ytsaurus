#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void RefCountedSingletonDestroyer(void* ctx)
{
    std::atomic<T*>* obj = reinterpret_cast<std::atomic<T*>*>(ctx);
    T* objToUnref = (*obj).load();
    objToUnref->Unref();
    *obj = reinterpret_cast<T*>(-1);
}

template <class T>
TIntrusivePtr<T> RefCountedSingleton()
{
    static std::atomic<T*> instance;

    // Failure here means that singleton is requested after it has been destroyed.
    YASSERT(instance != reinterpret_cast<T*>(-1));

    if (LIKELY(instance)) {
        return instance.load();
    }

    static TSpinLock spinLock;
    TGuard<TSpinLock> guard(spinLock);

    if (instance) {
        return instance.load();
    }

    T* obj = new T();
    obj->Ref();

    instance = obj;

    AtExit(
        RefCountedSingletonDestroyer<T>,
        &instance,
        TSingletonTraits<T>::Priority);

    return instance.load();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
