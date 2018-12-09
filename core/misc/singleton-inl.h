#pragma once
#ifndef SINGLETON_INL_H_
#error "Direct inclusion of this file is not allowed, include singleton.h"
// For the sake of sane code completion.
#include "singleton.h"
#endif

#include <atomic>
#include <mutex>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> RefCountedSingleton()
{
    static std::atomic<T*> RawInstance;
    auto* rawInstance = RawInstance.load(std::memory_order_acquire);
    if (Y_LIKELY(rawInstance)) {
        return rawInstance;
    }

    static TIntrusivePtr<T> StrongInstance;
    static std::once_flag Initialized;
    std::call_once(Initialized, [] {
        StrongInstance = New<T>();
        RawInstance.store(StrongInstance.Get());
    });

    return StrongInstance;
}

template <class T>
T* ImmortalSingleton()
{
    static std::aligned_storage_t<sizeof(T), alignof(T)> Storage;
    static std::once_flag Initialized;
    std::call_once(Initialized, [] {
        new (&Storage) T();
    });
    return reinterpret_cast<T*>(&Storage);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
