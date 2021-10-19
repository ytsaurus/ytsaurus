#pragma once
#ifndef SINGLETON_INL_H_
#error "Direct inclusion of this file is not allowed, include singleton.h"
// For the sake of sane code completion.
#include "singleton.h"
#endif

#include <atomic>
#include <type_traits>
#include <mutex>

#include <util/system/sanitizers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T* LeakySingleton()
{
#if defined(_asan_enabled_)
    static T* Ptr = [] {
        auto ptr = new T();
        NSan::MarkAsIntentionallyLeaked(ptr);
        return ptr;
    }();
    return Ptr;
#else
    static std::aligned_storage_t<sizeof(T), alignof(T)> Storage;
    static std::once_flag Initialized;
    std::call_once(Initialized, [] {
        new (&Storage) T();
    });
    return reinterpret_cast<T*>(&Storage);
#endif
}

template <class T>
TIntrusivePtr<T> LeakyRefCountedSingleton()
{
    static std::atomic<T*> Ptr;
    auto* ptr = Ptr.load(std::memory_order_acquire);
    if (Y_LIKELY(ptr)) {
        return ptr;
    }

    static std::once_flag Initialized;
    std::call_once(Initialized, [] {
        auto ptr = New<T>();
        Ref(ptr.Get());
        Ptr.store(ptr.Get());
#if defined(_asan_enabled_)
        NSan::MarkAsIntentionallyLeaked(ptr.Get());
#endif
    });

    return Ptr.load();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
