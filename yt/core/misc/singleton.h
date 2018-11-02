#pragma once

#include "common.h"

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> RefCountedSingleton()
{
    static std::atomic<T*> instance;
    auto* relaxedInstance = instance.load(std::memory_order_acquire);

    if (Y_LIKELY(relaxedInstance)) {
        return relaxedInstance;
    }

    static TSpinLock spinLock;
    static TIntrusivePtr<T> holder;

    auto guard = Guard(spinLock);

    auto* orderedInstance = instance.load();
    if (orderedInstance) {
        return orderedInstance;
    }

    YCHECK(!holder);
    holder = New<T>();
    instance.store(holder.Get());

    return holder;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
