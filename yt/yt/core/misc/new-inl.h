#pragma once
#ifndef NEW_INL_H_
#error "Direct inclusion of this file is not allowed, include new.h"
// For the sake of sane code completion.
#include "new.h"
#endif

#include <library/cpp/ytalloc/api/ytalloc.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedCookieHolder
{
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTypeCookie Cookie = NullRefCountedTypeCookie;

    void InitializeTracking(TRefCountedTypeCookie cookie)
    {
        YT_ASSERT(Cookie == NullRefCountedTypeCookie);
        Cookie = cookie;
        TRefCountedTrackerFacade::AllocateInstance(Cookie);
    }

    ~TRefCountedCookieHolder()
    {
        if (Cookie != NullRefCountedTypeCookie) {
            TRefCountedTrackerFacade::FreeInstance(Cookie);
        }
    }
#endif
};

template <class T>
struct TRefCountedWrapper final
    : public T
    , public TRefTracked<T>
{
    template <class... TArgs>
    explicit TRefCountedWrapper(TArgs&&... args)
        : T(std::forward<TArgs>(args)...)
    { }

    ~TRefCountedWrapper() = default;

    virtual void DestroyRefCounted() override
    {
        T::DestroyRefCountedImpl(this);
    }
};

template <class T, class TDeleter>
class TRefCountedWrapperWithDeleter final
    : public T
    , public TRefTracked<T>
{
public:
    template <class... TArgs>
    explicit TRefCountedWrapperWithDeleter(const TDeleter& deleter, TArgs&&... args)
        : T(std::forward<TArgs>(args)...)
        , Deleter_(deleter)
    { }

    ~TRefCountedWrapperWithDeleter() = default;

    virtual void DestroyRefCounted() override
    {
        Deleter_(this);
    }

private:
    TDeleter Deleter_;
};

template <class T>
struct TRefCountedWrapperWithCookie final
    : public T
    , public TRefCountedCookieHolder
{
    template <class... TArgs>
    explicit TRefCountedWrapperWithCookie(TArgs&&... args)
        : T(std::forward<TArgs>(args)...)
    { }

    ~TRefCountedWrapperWithCookie() = default;

    virtual void DestroyRefCounted() override
    {
        T::DestroyRefCountedImpl(this);
    }
};

namespace NDetail {

template <class T, class... As>
Y_FORCE_INLINE T* NewEpilogue(void* ptr, As&& ... args)
{
    try {
        auto* instance = static_cast<T*>(ptr);
        new (instance) T(std::forward<As>(args)...);
        return instance;
    } catch (const std::exception& ex) {
        // Do not forget to free the memory.
        TFreeMemory<T>::Do(ptr);
        throw;
    }
}

template <class T, bool = std::is_base_of_v<TRefCountedBase, T>>
struct TRefCountedHelper
{
    static constexpr size_t Size =  sizeof(TRefCounter) + sizeof(T);

    template <class... As>
    Y_FORCE_INLINE static T* Construct(void* ptr, As&&... args)
    {
        auto* refCounter = static_cast<TRefCounter*>(ptr);
        new (refCounter) TRefCounter();
        return new (reinterpret_cast<T*>(refCounter + 1)) T(std::forward<As>(args)...);
    }
};

template <class T>
struct TRefCountedHelper<T, true>
{
    static constexpr size_t Size = sizeof(TRefCountedWrapper<T>);

    template <class... As>
    Y_FORCE_INLINE static TRefCountedWrapper<T>* Construct(void* ptr, As&&... args)
    {
        using TDerived = TRefCountedWrapper<T>;
        return new (static_cast<TDerived*>(ptr)) TDerived(std::forward<As>(args)...);
    }
};

template <class T, class... As>
Y_FORCE_INLINE TIntrusivePtr<T> SafeConstruct(void* ptr, As&&... args)
{
    try {
        auto* instance = TRefCountedHelper<T>::Construct(ptr, std::forward<As>(args)...);
        return TIntrusivePtr<T>(instance, false);
    } catch (const std::exception& ex) {
        // Do not forget to free the memory.
        TFreeMemory<T>::Do(ptr);
        throw;
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T, class... As, class>
Y_FORCE_INLINE TIntrusivePtr<T> New(
    As&&... args)
{
    auto* ptr = NYTAlloc::AllocateConstSize<NDetail::TRefCountedHelper<T>::Size>();
    return NDetail::SafeConstruct<T>(ptr, std::forward<As>(args)...);
}

template <class T, class... As, class>
Y_FORCE_INLINE TIntrusivePtr<T> New(
    typename T::TAllocator* allocator,
    As&&... args)
{
    auto* ptr = allocator->Allocate(NDetail::TRefCountedHelper<T>::Size);
    return NDetail::SafeConstruct<T>(ptr, std::forward<As>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class... As, class>
Y_FORCE_INLINE TIntrusivePtr<T> NewWithExtraSpace(
    size_t extraSpaceSize,
    As&&... args)
{
    auto totalSize = NDetail::TRefCountedHelper<T>::Size + extraSpaceSize;
    auto* ptr = NYTAlloc::Allocate(totalSize);
    return NDetail::SafeConstruct<T>(ptr, std::forward<As>(args)...);
}

template <class T, class... As, class>
Y_FORCE_INLINE TIntrusivePtr<T> NewWithExtraSpace(
    typename T::TAllocator* allocator,
    size_t extraSpaceSize,
    As&&... args)
{
    auto totalSize = NDetail::TRefCountedHelper<T>::Size + extraSpaceSize;
    auto* ptr = allocator->Allocate(totalSize);
    return NDetail::SafeConstruct<T>(ptr, std::forward<As>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

// Support for polymorphic only
template <class T, class TDeleter, class... As>
Y_FORCE_INLINE TIntrusivePtr<T> NewWithDelete(const TDeleter& deleter, As&&... args)
{
    auto* ptr = NYTAlloc::AllocateConstSize<sizeof(TRefCountedWrapperWithDeleter<T, TDeleter>)>();
    auto* instance = NDetail::NewEpilogue<TRefCountedWrapperWithDeleter<T, TDeleter>>(
        ptr,
        deleter,
        std::forward<As>(args)...);

    return TIntrusivePtr<T>(instance, false);
}

////////////////////////////////////////////////////////////////////////////////

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
