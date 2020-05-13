#pragma once

#include "hazard_ptr.h"
#include "intrusive_ptr.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> MakeStrong(const THazardPtr<T>& ptr);

////////////////////////////////////////////////////////////////////////////////

// Operators * and -> for TAtomicPtr are useless because it is not safe to work with atomic ptr such way
// Safe usage is to convert to TIntrusivePtr. Not HazardPtr. HazardPtr can only be used to try ref count.

template <class T>
class TAtomicPtr
{
public:
    TAtomicPtr() = default;

    TAtomicPtr(std::nullptr_t);

    explicit TAtomicPtr(TIntrusivePtr<T> other);

    TAtomicPtr(TAtomicPtr&& other);

    ~TAtomicPtr();

    TAtomicPtr& operator=(TIntrusivePtr<T> other);

    TAtomicPtr& operator=(std::nullptr_t);

    TIntrusivePtr<T> Release();

    TIntrusivePtr<T> AcquireWeak() const;

    TIntrusivePtr<T> Acquire() const;

    TIntrusivePtr<T> Exchange(TIntrusivePtr<T>&& other);

    TIntrusivePtr<T> SwapIfCompare(THazardPtr<T>& compare, TIntrusivePtr<T> target);

    TIntrusivePtr<T> SwapIfCompare(T* comparePtr, TIntrusivePtr<T> target);

    TIntrusivePtr<T> SwapIfCompare(const TIntrusivePtr<T>& compare, TIntrusivePtr<T> target);

    bool SwapIfCompare(const TIntrusivePtr<T>& compare, TIntrusivePtr<T>* target);

    explicit operator bool() const;

private:
    template <class U>
    friend bool operator==(const TAtomicPtr<U>& lhs, const TIntrusivePtr<U>& rhs);

    template <class U>
    friend bool operator==(const TIntrusivePtr<U>& lhs, const TAtomicPtr<U>& rhs);

    template <class U>
    friend bool operator!=(const TAtomicPtr<U>& lhs, const TIntrusivePtr<U>& rhs);

    template <class U>
    friend bool operator!=(const TIntrusivePtr<U>& lhs, const TAtomicPtr<U>& rhs);

    std::atomic<T*> Ptr_ = {nullptr};
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ATOMIC_PTR_INL_H_
#include "atomic_ptr-inl.h"
#undef ATOMIC_PTR_INL_H_
