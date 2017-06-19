#pragma once
#ifndef REF_COUNTED_INL_H_
#error "Direct inclusion of this file is not allowed, include ref_counted.h"
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
    TRefCountedTypeCookie typeCookie,
    size_t instanceSize)
{
    object->InitializeTracking(typeCookie, instanceSize);
}

void RefCountedTrackerAllocate(
    TRefCountedTypeCookie cookie,
    size_t instanceSize);
void RefCountedTrackerFree(
    TRefCountedTypeCookie cookie,
    size_t instanceSize);

#endif

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void TRefCounter<false>::Ref() noexcept
{
    auto oldStrongCount = StrongCount_.fetch_add(1, std::memory_order_relaxed);
    Y_ASSERT(oldStrongCount > 0);
}

Y_FORCE_INLINE void TRefCounter<false>::Unref(const TRefCountedBase* object)
{
    auto oldStrongCount = StrongCount_.fetch_sub(1, std::memory_order_relaxed);
    Y_ASSERT(oldStrongCount > 0);

    if (oldStrongCount == 1) {
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

Y_FORCE_INLINE TRefCountedBase::~TRefCountedBase() noexcept
{
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    FinalizeTracking();
#endif
}

#ifdef YT_ENABLE_REF_COUNTED_TRACKING

Y_FORCE_INLINE void TRefCountedBase::InitializeTracking(
    TRefCountedTypeCookie typeCookie,
    size_t instanceSize)
{
    Y_ASSERT(TypeCookie_ == NullRefCountedTypeCookie);
    TypeCookie_ = typeCookie;

    Y_ASSERT(InstanceSize_ == 0);
    Y_ASSERT(instanceSize != 0);
    InstanceSize_ = instanceSize;

    NDetail::RefCountedTrackerAllocate(typeCookie, instanceSize);
}

Y_FORCE_INLINE void TRefCountedBase::FinalizeTracking()
{
    Y_ASSERT(TypeCookie_ != NullRefCountedTypeCookie);
    Y_ASSERT(InstanceSize_ != 0);
    NDetail::RefCountedTrackerFree(TypeCookie_, InstanceSize_);
}

#endif

////////////////////////////////////////////////////////////////////////////////

template <bool EnableWeak>
Y_FORCE_INLINE TRefCountedImpl<EnableWeak>::~TRefCountedImpl() noexcept
{
    // Failure here typically indicates an attempt to throw an exception
    // from ctor of ref-counted type.
    Y_ASSERT(GetRefCount() == 0);
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

