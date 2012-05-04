#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void RefCountedSingletonDestroyer(void* ctx)
{
    T** obj = reinterpret_cast<T**>(ctx);
    (*obj)->Unref();
    *obj = reinterpret_cast<T*>(-1);
}

template <class T>
TIntrusivePtr<T> RefCountedSingleton()
{
    static T* volatile instance;

    // Failure here means that singleton is requested after it has been destroyed.
    YASSERT(instance != reinterpret_cast<T*>(-1));

    if (EXPECT_TRUE(instance)) {
        return instance;
    }

    static TSpinLock spinLock;
    TGuard<TSpinLock> guard(spinLock);

    if (instance) {
        return instance;
    }

    T* obj = new T();
    obj->Ref();

    instance = obj;

    AtExit(
        RefCountedSingletonDestroyer<T>,
        const_cast<T**>(&instance),
        TSingletonTraits<T>::Priority);

    return instance;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
