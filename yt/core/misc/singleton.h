#pragma once

#include "common.h"

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// XXX(babenko): this singleton never dies
template <class T>
TIntrusivePtr<T> RefCountedSingleton()
{
    static std::atomic<T*> holder;

    auto* relaxedInstance = holder.load(std::memory_order_relaxed);

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

    holder.store(newInstance);

    return newInstance;
}

template <class T>
struct TSingletonWithFlag
{
    static std::atomic<bool> SingletonWasCreated_;

    static T* Get()
    {
        SingletonWasCreated_.store(true, std::memory_order_relaxed);
        return Singleton<T>();
    }

    static bool WasCreated()
    {
        return SingletonWasCreated_.load(std::memory_order_seq_cst);
    }
};

template <class T>
std::atomic<bool> TSingletonWithFlag<T>::SingletonWasCreated_ = ATOMIC_VAR_INIT(false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
