#pragma once

#include "hazard_ptr.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TAtomicRefCounter
{
    mutable std::atomic<int> Count = {1};
};

template <class T>
T* AcquireRef(T* ptr);

template <class T>
T* AcquireRef(const THazardPtr<T>& ptr);

template <class TTraits, class T>
void ReleaseRef(T* ptr);

struct TDefaultAllocator
{
    static void* Allocate(size_t size)
    {
        return NYTAlloc::Allocate(size);
    }

    static void Free(void* ptr)
    {
        NYTAlloc::Free(ptr);
    }
};

template <class T, class TTraits = TDefaultAllocator>
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

template <class T, class TTraits, class... As>
TRefCountedPtr<T, TTraits> CreateObjectWithExtraSpace(
    TTraits* traits,
    size_t extraSpaceSize,
    As&&... args);

template <class T, class... As>
TRefCountedPtr<T> CreateObject(As&&... args);

////////////////////////////////////////////////////////////////////////////////

// Operators * and -> for TAtomicPtr are useless because it is not safe to work with atomic ptr such way
// Safe usage is to convert to TRefCountedPtr. Not HazardPtr. HazardPtr can only be used to try ref count.

template <class T, class TTraits = TDefaultAllocator>
class TAtomicPtr
{
public:
    TAtomicPtr() = default;

    TAtomicPtr(std::nullptr_t);

    explicit TAtomicPtr(TRefCountedPtr<T, TTraits> other);

    TAtomicPtr(TAtomicPtr&& other);

    ~TAtomicPtr();

    TAtomicPtr& operator=(TRefCountedPtr<T, TTraits> other);

    TAtomicPtr& operator=(std::nullptr_t);

    TRefCountedPtr<T, TTraits> Release();

    TRefCountedPtr<T, TTraits> AcquireWeak() const;

    TRefCountedPtr<T, TTraits> Acquire() const;

    TRefCountedPtr<T, TTraits> Exchange(TRefCountedPtr<T, TTraits>&& other);

    TRefCountedPtr<T, TTraits> SwapIfCompare(THazardPtr<T>& compare, TRefCountedPtr<T, TTraits> target);

    TRefCountedPtr<T, TTraits> SwapIfCompare(T* comparePtr, TRefCountedPtr<T, TTraits> target);

    TRefCountedPtr<T, TTraits> SwapIfCompare(const TRefCountedPtr<T, TTraits>& compare, TRefCountedPtr<T, TTraits> target);

    bool SwapIfCompare(const TRefCountedPtr<T>& compare, TRefCountedPtr<T, TTraits>* target);

    explicit operator bool() const;

private:
    template <class U, class UAlloc>
    friend bool operator==(const TAtomicPtr<U, UAlloc>& lhs, const TRefCountedPtr<U, UAlloc>& rhs);

    template <class U, class UAlloc>
    friend bool operator==(const TRefCountedPtr<U, UAlloc>& lhs, const TAtomicPtr<U, UAlloc>& rhs);

    template <class U, class UAlloc>
    friend bool operator!=(const TAtomicPtr<U, UAlloc>& lhs, const TRefCountedPtr<U, UAlloc>& rhs);

    template <class U, class UAlloc>
    friend bool operator!=(const TRefCountedPtr<U, UAlloc>& lhs, const TAtomicPtr<U, UAlloc>& rhs);

    std::atomic<T*> Ptr_ = {nullptr};
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ATOMIC_PTR_INL_H_
#include "atomic_ptr-inl.h"
#undef ATOMIC_PTR_INL_H_
