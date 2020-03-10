#pragma once

#include "hazard_ptr.h"
#include "allocator_traits.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TAtomicRefCounter
{
    mutable std::atomic<int> Count = {1};
    TDeleterBase* Deleter;

    explicit TAtomicRefCounter(TDeleterBase* deleter) noexcept
        : Deleter(deleter)
    { }
};

template <class T>
T* AcquireRef(T* ptr);

template <class T>
T* AcquireRef(const THazardPtr<T>& ptr);

template <class TTraits, class T>
void ReleaseRef(T* ptr);

template <class T>
class TRefCountedPtr
{
public:
    TRefCountedPtr() = default;

    TRefCountedPtr(std::nullptr_t);

    explicit TRefCountedPtr(T* obj, bool addReference = true);

    explicit TRefCountedPtr(const THazardPtr<T>& ptr);

    TRefCountedPtr(const TRefCountedPtr& other);

    TRefCountedPtr(TRefCountedPtr&& other);

    ~TRefCountedPtr();

    TRefCountedPtr& operator=(TRefCountedPtr other);

    TRefCountedPtr& operator=(std::nullptr_t);

    TRefCountedPtr Exchange(TRefCountedPtr&& other);

    T* Release();

    T* Get() const;

    T& operator*() const;

    T* operator->() const;

    explicit operator bool() const;

private:
    T* Ptr_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class TAllocator, class... As>
TRefCountedPtr<T> CreateObjectWithExtraSpace(
    TAllocator* allocator,
    size_t extraSpaceSize,
    As&&... args);

template <class T, class... As>
TRefCountedPtr<T> CreateObject(As&&... args);

////////////////////////////////////////////////////////////////////////////////

// Operators * and -> for TAtomicPtr are useless because it is not safe to work with atomic ptr such way
// Safe usage is to convert to TRefCountedPtr. Not HazardPtr. HazardPtr can only be used to try ref count.

template <class T>
class TAtomicPtr
{
public:
    TAtomicPtr() = default;

    TAtomicPtr(std::nullptr_t);

    explicit TAtomicPtr(TRefCountedPtr<T> other);

    TAtomicPtr(TAtomicPtr&& other);

    ~TAtomicPtr();

    TAtomicPtr& operator=(TRefCountedPtr<T> other);

    TAtomicPtr& operator=(std::nullptr_t);

    TRefCountedPtr<T> Release();

    TRefCountedPtr<T> AcquireWeak() const;

    TRefCountedPtr<T> Acquire() const;

    TRefCountedPtr<T> Exchange(TRefCountedPtr<T>&& other);

    TRefCountedPtr<T> SwapIfCompare(THazardPtr<T>& compare, TRefCountedPtr<T> target);

    TRefCountedPtr<T> SwapIfCompare(T* comparePtr, TRefCountedPtr<T> target);

    TRefCountedPtr<T> SwapIfCompare(const TRefCountedPtr<T>& compare, TRefCountedPtr<T> target);

    bool SwapIfCompare(const TRefCountedPtr<T>& compare, TRefCountedPtr<T>* target);

    explicit operator bool() const;

private:
    template <class U>
    friend bool operator==(const TAtomicPtr<U>& lhs, const TRefCountedPtr<U>& rhs);

    template <class U>
    friend bool operator==(const TRefCountedPtr<U>& lhs, const TAtomicPtr<U>& rhs);

    template <class U>
    friend bool operator!=(const TAtomicPtr<U>& lhs, const TRefCountedPtr<U>& rhs);

    template <class U>
    friend bool operator!=(const TRefCountedPtr<U>& lhs, const TAtomicPtr<U>& rhs);

    std::atomic<T*> Ptr_ = {nullptr};
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ATOMIC_PTR_INL_H_
#include "atomic_ptr-inl.h"
#undef ATOMIC_PTR_INL_H_
