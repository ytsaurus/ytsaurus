#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template<class T>
void RefCountedSingletonDestroyer(void* ctx)
{
    T** obj = reinterpret_cast<T**>(ctx);
    (*obj)->UnRef();
    *obj = reinterpret_cast<T*>(-1);
}

template<class T>
TIntrusivePtr<T> RefCountedSingleton()
{
    static T* volatile instance;

    YASSERT(instance != reinterpret_cast<T*>(-1));

    if (EXPECT_TRUE(instance != NULL)) {
        return instance;
    }

    static TSpinLock spinLock;
    TGuard<TSpinLock> guard(spinLock);

    if (instance != NULL) {
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
