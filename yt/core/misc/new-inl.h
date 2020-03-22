#pragma once
#ifndef NEW_INL_H_
#error "Direct inclusion of this file is not allowed, include new.h"
// For the sake of sane code completion.
#include "new.h"
#endif

#include <library/cpp/ytalloc/api/ytalloc.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

//! A per-translation unit tag type.
struct TCurrentTranslationUnitTag
{ };

} // namespace

template <class T>
TRefCountedTypeKey GetRefCountedTypeKey()
{
    return &typeid(T);
}

template <class T>
Y_FORCE_INLINE TRefCountedTypeCookie GetRefCountedTypeCookie()
{
    static std::atomic<TRefCountedTypeCookie> cookie{NullRefCountedTypeCookie};
    auto cookieValue = cookie.load(std::memory_order_relaxed);
    if (Y_UNLIKELY(cookieValue == NullRefCountedTypeCookie)) {
        cookieValue = TRefCountedTrackerFacade::GetCookie(
            GetRefCountedTypeKey<T>(),
            sizeof(T),
            NYT::TSourceLocation());
        cookie.store(cookieValue, std::memory_order_relaxed);
    }
    return cookieValue;
}

template <class T, class TTag, int Counter>
Y_FORCE_INLINE TRefCountedTypeCookie GetRefCountedTypeCookieWithLocation(const TSourceLocation& location)
{
    static std::atomic<TRefCountedTypeCookie> cookie{NullRefCountedTypeCookie};
    auto cookieValue = cookie.load(std::memory_order_relaxed);
    if (Y_UNLIKELY(cookieValue == NullRefCountedTypeCookie)) {
        cookieValue = TRefCountedTrackerFacade::GetCookie(
            GetRefCountedTypeKey<T>(),
            sizeof(T),
            location);
        cookie.store(cookieValue, std::memory_order_relaxed);
    }
    return cookieValue;
}

template <class T>
void DestroyRefCountedImpl(T* ptr)
{
    auto* base = static_cast<TRefCountedImpl*>(ptr);
    void** vTablePtr = reinterpret_cast<void**>(base);
    // Dtor is virtual.
    ptr->~T();
    *vTablePtr = ptr;

    base->WeakUnref();
}

template <class T>
struct TRefCountedWrapper final
    : public T
{
    using TSelf = TRefCountedWrapper;

    friend void DestroyRefCountedImpl<TSelf>(TSelf* ptr);

    template <class... TArgs>
    explicit TRefCountedWrapper(TArgs&&... args)
        : T(std::forward<TArgs>(args)...)
    {
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        auto typeCookie = GetRefCountedTypeCookie<T>();
        TRefCountedTrackerFacade::AllocateInstance(typeCookie);
#endif
    }

    void DestroyRefCounted()
    {
        DestroyRefCountedImpl(this);
    }

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    ~TRefCountedWrapper()
    {
        auto typeCookie = GetRefCountedTypeCookie<T>();
        TRefCountedTrackerFacade::FreeInstance(typeCookie);
    }
#endif
};

template <class T>
struct TRefCountedWrapperWithCookie final
    : public T
{
    using TSelf = TRefCountedWrapperWithCookie;

    friend void DestroyRefCountedImpl<TSelf>(TSelf* ptr);

    template <class... TArgs>
    explicit TRefCountedWrapperWithCookie(TArgs&&... args)
        : T(std::forward<TArgs>(args)...)
    { }

    void DestroyRefCounted()
    {
        DestroyRefCountedImpl(this);
    }

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTypeCookie Cookie = NullRefCountedTypeCookie;

    void InitializeTracking(TRefCountedTypeCookie cookie)
    {
        YT_ASSERT(Cookie == NullRefCountedTypeCookie);
        Cookie = cookie;
        TRefCountedTrackerFacade::AllocateInstance(Cookie);
    }

    ~TRefCountedWrapperWithCookie()
    {
        if (Cookie != NullRefCountedTypeCookie) {
            TRefCountedTrackerFacade::FreeInstance(Cookie);
        }
    }
#endif
};

namespace NDetail {

template <class T, class... As>
Y_FORCE_INLINE T* NewEpilogue(
    void* ptr,
    As&& ... args)
{
    auto* instance = static_cast<T*>(ptr);

    try {
        new (instance) T(std::forward<As>(args)...);
    } catch (const std::exception& ex) {
        // Do not forget to free the memory.
        NYTAlloc::FreeNonNull(ptr);
        throw;
    }

    return instance;
}

} // namespace NDetail

template <class T, class... As>
Y_FORCE_INLINE TIntrusivePtr<T> New(As&&... args)
{
    auto* ptr = NYTAlloc::AllocateConstSize<sizeof(TRefCountedWrapper<T>)>();
    auto* instance = NDetail::NewEpilogue<TRefCountedWrapper<T>>(ptr, std::forward<As>(args)...);

    return TIntrusivePtr<T>(instance, false);
}

template <class T, class... As>
TIntrusivePtr<T> NewWithExtraSpace(
    size_t extraSpaceSize,
    As&&... args)
{
    auto totalSize = sizeof(TRefCountedWrapper<T>) + extraSpaceSize;
    auto* ptr = NYTAlloc::Allocate(totalSize);
    auto* instance = NDetail::NewEpilogue<TRefCountedWrapper<T>>(ptr, std::forward<As>(args)...);

    return TIntrusivePtr<T>(instance, false);
}

template <class T, class TTag, int Counter, class... As>
Y_FORCE_INLINE TIntrusivePtr<T> NewWithLocation(
    const TSourceLocation& location,
    As&&... args)
{
    auto* ptr = NYTAlloc::AllocateConstSize<sizeof(TRefCountedWrapperWithCookie<T>)>();
    auto* instance = NDetail::NewEpilogue<TRefCountedWrapperWithCookie<T>>(ptr, std::forward<As>(args)...);

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    instance->InitializeTracking(GetRefCountedTypeCookieWithLocation<T, TTag, Counter>(location));
#else
    Y_UNUSED(location);
#endif

    return TIntrusivePtr<T>(instance, false);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
const void* TWithExtraSpace<T>::GetExtraSpacePtr() const
{
    return static_cast<const T*>(this) + 1;
}

template <class T>
void* TWithExtraSpace<T>::GetExtraSpacePtr()
{
    return static_cast<T*>(this) + 1;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
