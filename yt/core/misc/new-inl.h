#pragma once
#ifndef NEW_INL_H_
#error "Direct inclusion of this file is not allowed, include new.h"
// For the sake of sane code completion.
#include "new.h"
#endif

#include "yt_alloc.h"

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
    if (Y_UNLIKELY(cookie == NullRefCountedTypeCookie)) {
        cookie = TRefCountedTrackerFacade::GetCookie(
            GetRefCountedTypeKey<T>(),
            sizeof(T),
            NYT::TSourceLocation());
    }
    return cookie;
}

template <class T, class TTag, int Counter>
Y_FORCE_INLINE TRefCountedTypeCookie GetRefCountedTypeCookieWithLocation(const TSourceLocation& location)
{
    static std::atomic<TRefCountedTypeCookie> cookie{NullRefCountedTypeCookie};
    if (Y_UNLIKELY(cookie == NullRefCountedTypeCookie)) {
        cookie = TRefCountedTrackerFacade::GetCookie(
            GetRefCountedTypeKey<T>(),
            sizeof(T),
            location);
    }
    return cookie;
}

namespace NDetail {

Y_FORCE_INLINE void InitializeNewInstance(
    void* /*instance*/,
    void* /*ptr*/)
{ }

Y_FORCE_INLINE void InitializeNewInstance(
    TRefCounted* instance,
    void* ptr)
{
    instance->GetRefCounter()->SetPtr(ptr);
}

template <class T, class... As>
Y_FORCE_INLINE TIntrusivePtr<T> NewEpilogue(
    void* ptr,
    TRefCountedTypeCookie cookie,
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

    InitializeNewInstance(instance, ptr);
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    InitializeRefCountedTracking(instance, cookie);
#else
    Y_UNUSED(cookie);
#endif

    return TIntrusivePtr<T>(instance, false);
}

} // namespace NDetail

template <class T, class... As>
Y_FORCE_INLINE TIntrusivePtr<T> New(As&&... args)
{
    auto* ptr = NYTAlloc::AllocateConstSize<sizeof(T)>();
    return NDetail::NewEpilogue<T>(
        ptr,
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        GetRefCountedTypeCookie<T>(),
#else
        NullRefCountedTypeCookie, // unused
#endif
        std::forward<As>(args)...);
}

template <class T, class... As>
TIntrusivePtr<T> NewWithExtraSpace(
    size_t extraSpaceSize,
    As&&... args)
{
    auto totalSize = sizeof(T) + extraSpaceSize;
    auto* ptr = NYTAlloc::Allocate(totalSize);
    return NDetail::NewEpilogue<T>(
        ptr,
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        GetRefCountedTypeCookie<T>(),
#else
        NullRefCountedTypeCookie, // unused
#endif
        std::forward<As>(args)...);
}

template <class T, class TTag, int Counter, class... As>
Y_FORCE_INLINE TIntrusivePtr<T> NewWithLocation(
    const TSourceLocation& location,
    As&&... args)
{
    auto* ptr = NYTAlloc::AllocateConstSize<sizeof(T)>();
    return NDetail::NewEpilogue<T>(
        ptr,
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        GetRefCountedTypeCookieWithLocation<T, TTag, Counter>(location),
#else
        NullRefCountedTypeCookie, // unused
#endif
        std::forward<As>(args)...);
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
