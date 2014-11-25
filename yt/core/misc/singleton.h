#pragma once

#include "common.h"

#include <atomic>

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
    static std::atomic<T*> holder;

    auto* relaxedInstance = holder.load(std::memory_order_relaxed);

    // Failure here means that singleton is requested after it has been destroyed.
    YASSERT(relaxedInstance != reinterpret_cast<T*>(-1));

    if (LIKELY(relaxedInstance)) {
        return relaxedInstance;
    }

    static TSpinLock spinLock;
    TGuard<TSpinLock> guard(spinLock);

    auto* orderedInstance = holder.load();
    if (orderedInstance) {
        return orderedInstance;
    }

    auto* newInstance = new T();
    newInstance->Ref();

    holder.store(newInstance);

    AtExit(
        RefCountedSingletonDestroyer<T>,
        &holder,
        TSingletonTraits<T>::Priority);

    return newInstance;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
