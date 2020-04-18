#pragma once
#ifndef REF_COUNTED_INL_H_
#error "Direct inclusion of this file is not allowed, include ref_counted.h"
// For the sake of sane code completion.
#include "ref_counted.h"
#endif

#include <library/cpp/ytalloc/api/ytalloc.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void TRefCountedBase::operator delete(void* ptr) noexcept
{
    NYTAlloc::FreeNonNull(ptr);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE int TRefCountedLite::Ref() const noexcept
{
    // It is safe to use relaxed here, since new reference is always created from another live reference.
    return StrongCount_.fetch_add(1, std::memory_order_relaxed);
}

Y_FORCE_INLINE void TRefCountedLite::Unref() const
{
    auto oldStrongCount = StrongCount_.fetch_sub(1, std::memory_order_release);
    YT_ASSERT(oldStrongCount > 0);

    if (oldStrongCount == 1) {
        // We must properly synchronize last access to object with it destruction.
        // Otherwise compiler might reorder access to object past this decrement.
        //
        // See http://www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html#boost_atomic.usage_examples.example_reference_counters
        //
        StrongCount_.load(std::memory_order_acquire);

        const_cast<TRefCountedLite*>(this)->DestroyRefCounted();
    }
}

Y_FORCE_INLINE int TRefCountedLite::GetRefCount() const noexcept
{
    return StrongCount_.load(std::memory_order_relaxed);
}

Y_FORCE_INLINE int AtomicallyIncrementIfNonZero(std::atomic<int>& atomic)
{
    // Atomically performs the following:
    // { auto v = *p; if (v != 0) ++(*p); return v; }
    auto value = atomic.load(std::memory_order_relaxed);

    while (value != 0 && !atomic.compare_exchange_weak(value, value + 1));

    return value;
}

Y_FORCE_INLINE bool TRefCountedLite::TryRef() const noexcept
{
    return AtomicallyIncrementIfNonZero(StrongCount_) > 0;
}

template <class T>
Y_FORCE_INLINE TIntrusivePtr<T> TRefCountedLite::DangerousGetPtr(T* object)
{
    return object->TryRef()
        ? TIntrusivePtr<T>(object, false)
        : TIntrusivePtr<T>();
}

template <class T>
void TRefCountedLite::DestroyRefCountedImpl(T* ptr)
{
    // No virtual call when T is final.
    ptr->~T();
    NYTAlloc::FreeNonNull(ptr);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void TRefCounted::Ref() const noexcept
{
    auto oldStrongCount = TRefCountedLite::Ref();
    YT_ASSERT(oldStrongCount > 0 && WeakCount_.load() > 0);
}

Y_FORCE_INLINE bool TRefCounted::TryRef() const noexcept
{
    YT_ASSERT(WeakCount_.load(std::memory_order_relaxed) > 0);
    return TRefCountedLite::TryRef();
}

Y_FORCE_INLINE void TRefCounted::WeakRef() const noexcept
{
    auto oldWeakCount = WeakCount_.fetch_add(1, std::memory_order_relaxed);
    YT_ASSERT(oldWeakCount > 0);
}

Y_FORCE_INLINE void TRefCounted::WeakUnref() const
{
    auto oldWeakCount = WeakCount_--;
    YT_ASSERT(oldWeakCount > 0);
    if (oldWeakCount == 1) {
        void** vTablePtr = reinterpret_cast<void**>(const_cast<TRefCounted*>(this));
        void* derived = *vTablePtr;
        NYTAlloc::FreeNonNull(derived);
    }
}

Y_FORCE_INLINE int TRefCounted::GetWeakRefCount() const noexcept
{
    return WeakCount_.load(std::memory_order_relaxed);
}

template <class T>
void TRefCounted::DestroyRefCountedImpl(T* ptr)
{
    auto* base = static_cast<TRefCounted*>(ptr);
    void** vTablePtr = reinterpret_cast<void**>(base);
    // No virtual call when T is final.
    ptr->~T();
    *vTablePtr = ptr;

    base->WeakUnref();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

