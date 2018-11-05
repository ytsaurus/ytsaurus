#pragma once
#ifndef REF_COUNTED_INL_H_
#error "Direct inclusion of this file is not allowed, include ref_counted.h"
// For the sake of sane code completion.
#include "ref_counted.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void TRefCountedBase::operator delete(void* ptr) noexcept
{
    ::free(ptr);
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

Y_FORCE_INLINE int AtomicallyIncrementIfNonZero(std::atomic<int>& atomic)
{
    // Atomically performs the following:
    // { auto v = *p; if (v != 0) ++(*p); return v; }
    while (true) {
        auto value = atomic.load();

        if (value == 0) {
            return value;
        }

        if (atomic.compare_exchange_weak(value, value + 1)) {
            return value;
        }
    }
}

#ifdef YT_ENABLE_REF_COUNTED_TRACKING

Y_FORCE_INLINE void InitializeRefCountedTracking(
    TRefCountedBase* object,
    TRefCountedTypeCookie typeCookie)
{
    object->InitializeTracking(typeCookie);
}

#endif

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void TRefCounter<false>::Ref() noexcept
{
    // It is safe to use relaxed here, since new reference is always created from another live reference.
    auto oldStrongCount = StrongCount_.fetch_add(1, std::memory_order_relaxed);
    Y_ASSERT(oldStrongCount > 0);
}

Y_FORCE_INLINE void TRefCounter<false>::Unref(const TRefCountedBase* object)
{
    // We must properly synchronize last access to object with it destruction.
    // Otherwise compiler might reorder access to object past this decrement.
    //
    // See http://www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html#boost_atomic.usage_examples.example_reference_counters
    //
    auto oldStrongCount = StrongCount_.fetch_sub(1, std::memory_order_release);
    Y_ASSERT(oldStrongCount > 0);

    if (oldStrongCount == 1) {
        StrongCount_.load(std::memory_order_acquire);
        DestroyAndDispose(object);
    }
}

Y_FORCE_INLINE bool TRefCounter<false>::TryRef() noexcept
{
    return AtomicallyIncrementIfNonZero(StrongCount_) > 0;
}

Y_FORCE_INLINE int TRefCounter<false>::GetRefCount() const noexcept
{
    return StrongCount_.load(std::memory_order_relaxed);
}

Y_FORCE_INLINE void TRefCounter<false>::DestroyAndDispose(const TRefCountedBase* object)
{
    // Dtor is virtual.
    // operator delete calls ::free.
    delete object;
}

////////////////////////////////////////////////////////////////////////////////

static_assert(
    std::is_trivially_destructible<TRefCounter<true>>::value,
    "TRefCounter<true> must be trivially destructible.");

Y_FORCE_INLINE void TRefCounter<true>::SetPtr(void* ptr) noexcept
{
    Y_ASSERT(!Ptr_);
    Ptr_ = ptr;
}

Y_FORCE_INLINE void TRefCounter<true>::Ref() noexcept
{
    auto oldStrongCount = StrongCount_++;
    Y_ASSERT(oldStrongCount > 0 && WeakCount_.load() > 0);
}

Y_FORCE_INLINE void TRefCounter<true>::Unref(const TRefCountedBase* object)
{
    auto oldStrongCount = StrongCount_--;
    Y_ASSERT(oldStrongCount > 0);

    if (oldStrongCount == 1) {
        Destroy(object);

        auto oldWeakCount = WeakCount_--;
        Y_ASSERT(oldWeakCount > 0);

        if (oldWeakCount == 1) {
            Dispose();
        }
    }
}

Y_FORCE_INLINE bool TRefCounter<true>::TryRef() noexcept
{
    Y_ASSERT(WeakCount_.load(std::memory_order_relaxed) > 0);
    return AtomicallyIncrementIfNonZero(StrongCount_) > 0;
}

Y_FORCE_INLINE void TRefCounter<true>::WeakRef() noexcept
{
    auto oldWeakCount = WeakCount_.fetch_add(1, std::memory_order_relaxed);
    Y_ASSERT(oldWeakCount > 0);
}

Y_FORCE_INLINE void TRefCounter<true>::WeakUnref()
{
    auto oldWeakCount = WeakCount_--;
    Y_ASSERT(oldWeakCount > 0);
    if (oldWeakCount == 1) {
        Dispose();
    }
}

Y_FORCE_INLINE int TRefCounter<true>::GetRefCount() const noexcept
{
    return StrongCount_.load(std::memory_order_relaxed);
}

Y_FORCE_INLINE int TRefCounter<true>::GetWeakRefCount() const noexcept
{
    return WeakCount_.load(std::memory_order_relaxed);
}

Y_FORCE_INLINE void TRefCounter<true>::Destroy(const TRefCountedBase* object)
{
    // Dtor is virtual.
    object->~TRefCountedBase();
}

Y_FORCE_INLINE void TRefCounter<true>::Dispose()
{
    Y_ASSERT(Ptr_);
    ::free(Ptr_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_ENABLE_REF_COUNTED_TRACKING

Y_FORCE_INLINE void TRefCountedBase::InitializeTracking(TRefCountedTypeCookie typeCookie)
{
    Y_ASSERT(TypeCookie_ == NullRefCountedTypeCookie);
    TypeCookie_ = typeCookie;
    TRefCountedTrackerFacade::AllocateInstance(typeCookie);
}

Y_FORCE_INLINE void TRefCountedBase::FinalizeTracking()
{
    Y_ASSERT(TypeCookie_ != NullRefCountedTypeCookie);
    TRefCountedTrackerFacade::FreeInstance(TypeCookie_);
}

#endif

////////////////////////////////////////////////////////////////////////////////

template <bool EnableWeak>
Y_FORCE_INLINE TRefCountedImpl<EnableWeak>::~TRefCountedImpl() noexcept
{
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    // NB: If we are still in the NewImpl(...), TypeCookie_ is still
    // NullRefCountedTypeCookie and the reference counter should be equal to
    // 1 since the first strong pointer is not created (and hence
    // destructed) yet.
    if (Y_UNLIKELY(TypeCookie_ == NullRefCountedTypeCookie)) {
        YCHECK(GetRefCount() == 1);
    } else {
        FinalizeTracking();
    }
#else
    YCHECK(GetRefCount() == 0);
#endif
}

template <bool EnableWeak>
Y_FORCE_INLINE void TRefCountedImpl<EnableWeak>::Ref() const noexcept
{
    RefCounter_.Ref();
}

template <bool EnableWeak>
Y_FORCE_INLINE void TRefCountedImpl<EnableWeak>::Unref() const
{
    RefCounter_.Unref(this);
}

template <bool EnableWeak>
Y_FORCE_INLINE int TRefCountedImpl<EnableWeak>::GetRefCount() const noexcept
{
    return RefCounter_.GetRefCount();
}

template <bool EnableWeak>
Y_FORCE_INLINE NDetail::TRefCounter<EnableWeak>* TRefCountedImpl<EnableWeak>::GetRefCounter() const noexcept
{
    return &RefCounter_;
}

template <bool EnableWeak>
template <class T>
Y_FORCE_INLINE TIntrusivePtr<T> TRefCountedImpl<EnableWeak>::DangerousGetPtr(T* object)
{
    return object->RefCounter_.TryRef()
        ? TIntrusivePtr<T>(object, false)
        : TIntrusivePtr<T>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

