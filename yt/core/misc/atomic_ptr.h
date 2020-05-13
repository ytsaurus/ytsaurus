#pragma once

#include "hazard_ptr.h"
#include "intrusive_ptr.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
using TRefCountedPtr = TIntrusivePtr<T>;

template <class T>
TIntrusivePtr<T> MakeStrong(const THazardPtr<T>& ptr);

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
