#pragma once

#include "intrusive_ptr.h"
#include "port.h"

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <util/generic/noncopyable.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSourceLocation;

class TRefCountedBase;

using TRefCountedTypeCookie = int;
const int NullRefCountedTypeCookie = -1;

using TRefCountedTypeKey = const void*;

////////////////////////////////////////////////////////////////////////////////

// Used to avoid including heavy ref_counted_tracker.h
class TRefCountedTrackerFacade
{
public:
    static TRefCountedTypeCookie GetCookie(
        TRefCountedTypeKey typeKey,
        size_t instanceSize,
        const NYT::TSourceLocation& location);

    static void AllocateInstance(TRefCountedTypeCookie cookie);
    static void FreeInstance(TRefCountedTypeCookie cookie);

    static void AllocateTagInstance(TRefCountedTypeCookie cookie);
    static void FreeTagInstance(TRefCountedTypeCookie cookie);

    static void AllocateSpace(TRefCountedTypeCookie cookie, size_t size);
    static void FreeSpace(TRefCountedTypeCookie cookie, size_t size);

    // Typically invoked from GDB console.
    // Dumps the ref-counted statistics sorted by "bytes alive".
    static void Dump();
};

////////////////////////////////////////////////////////////////////////////////

//! A technical base class for ref-counted objects and promise states.
class TRefCountedBase
{
public:
    TRefCountedBase() = default;
    virtual ~TRefCountedBase() noexcept = default;

    void operator delete(void* ptr) noexcept;

private:
    TRefCountedBase(const TRefCountedBase&) = delete;
    TRefCountedBase(TRefCountedBase&&) = delete;

    TRefCountedBase& operator=(const TRefCountedBase&) = delete;
    TRefCountedBase& operator=(TRefCountedBase&&) = delete;

};

////////////////////////////////////////////////////////////////////////////////

//! Base class for all reference-counted objects.
class TRefCountedLite
    : public TRefCountedBase
{
public:
    TRefCountedLite() = default;
    ~TRefCountedLite() noexcept = default;

    //! Increments the strong reference counter.
    int Ref() const noexcept;

    //! Decrements the strong reference counter.
    void Unref() const;

    //! Increments the strong reference counter if it is not null.
    bool TryRef() const noexcept;

    //! Returns current number of strong references to the object.
    /*!
     * Note that you should never ever use this method in production code.
     * This method is mainly for debugging purposes.
     */
    int GetRefCount() const noexcept;

private:
    //! Number of strong references.
    mutable std::atomic<int> StrongCount_ = 1;

    virtual void DestroyRefCounted() = 0;

protected:
    template <class T>
    void DestroyRefCountedImpl(T* ptr);
};

////////////////////////////////////////////////////////////////////////////////

class TRefCounted
    : public TRefCountedLite
{
public:
    //! Increments the strong reference counter.
    void Ref() const noexcept;

    //! Increments the strong reference counter if it is not null.
    bool TryRef() const noexcept;

    //! Increments the weak reference counter.
    void WeakRef() const noexcept;

    //! Decrements the weak reference counter.
    void WeakUnref() const;

    //! Returns current number of weak references to the object.
    int GetWeakRefCount() const noexcept;

private:
    //! Number of weak references plus one if there is at least one strong reference.
    mutable std::atomic<int> WeakCount_ = 1;

protected:
    template <class T>
    void DestroyRefCountedImpl(T* ptr);
};

using TIntrinsicRefCounted = TRefCountedLite;

////////////////////////////////////////////////////////////////////////////////

template <bool EnableWeak>
class TGenericRefCounted;

template <>
class TGenericRefCounted<false>
    : public TRefCountedLite
{ };

template <>
class TGenericRefCounted<true>
    : public TRefCounted
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define REF_COUNTED_INL_H_
#include "ref_counted-inl.h"
#undef REF_COUNTED_INL_H_
