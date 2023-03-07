#pragma once
#ifndef ATOMIC_PTR_INL_H_
#error "Direct inclusion of this file is not allowed, include atomic_ptr.h"
// For the sake of sane code completion.
#include "atomic_ptr.h"
#endif
#undef ATOMIC_PTR_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> MakeStrong(const THazardPtr<T>& ptr)
{
    if (ptr) {
        if (GetRefCounter(ptr.Get())->TryRef()) {
             return TIntrusivePtr<T>(ptr.Get(), false);
        } else {
            static const auto& Logger = LockFreePtrLogger;
            YT_LOG_TRACE("Failed to acquire ref (Ptr: %v)",
                ptr.Get());
        }
    }

    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TAtomicPtr<T>::TAtomicPtr(std::nullptr_t)
{ }

template <class T>
TAtomicPtr<T>::TAtomicPtr(TIntrusivePtr<T> other)
    : Ptr_(other.Release())
{ }

template <class T>
TAtomicPtr<T>::TAtomicPtr(TAtomicPtr&& other)
    : Ptr_(other.Ptr_)
{
    other.Ptr_ = nullptr;
}

template <class T>
TAtomicPtr<T>::~TAtomicPtr()
{
    auto ptr = Ptr_.load();
    if (ptr) {
        Unref(ptr);
    }
}

template <class T>
TAtomicPtr<T>& TAtomicPtr<T>::operator=(TIntrusivePtr<T> other)
{
    Exchange(std::move(other));
    return *this;
}

template <class T>
TAtomicPtr<T>& TAtomicPtr<T>::operator=(std::nullptr_t)
{
    Exchange(TIntrusivePtr<T>());
    return *this;
}

template <class T>
TIntrusivePtr<T> TAtomicPtr<T>::Release()
{
    return Exchange(TIntrusivePtr<T>());
}

template <class T>
TIntrusivePtr<T> TAtomicPtr<T>::AcquireWeak() const
{
    auto hazardPtr = THazardPtr<T>::Acquire([&] {
        return Ptr_.load(std::memory_order_relaxed);
    });
    return MakeStrong(hazardPtr);
}

template <class T>
TIntrusivePtr<T> TAtomicPtr<T>::Acquire() const
{
    while (auto hazardPtr = THazardPtr<T>::Acquire([&] {
        return Ptr_.load(std::memory_order_relaxed);
    })) {
        if (auto ptr = MakeStrong(hazardPtr)) {
            return ptr;
        }
    }

    return nullptr;
}

template <class T>
TIntrusivePtr<T> TAtomicPtr<T>::Exchange(TIntrusivePtr<T>&& other)
{
    auto oldPtr = Ptr_.exchange(other.Release());
    return TIntrusivePtr<T>(oldPtr, false);
}

template <class T>
TIntrusivePtr<T> TAtomicPtr<T>::SwapIfCompare(THazardPtr<T>& compare, TIntrusivePtr<T> target)
{
    auto comparePtr = compare.Get();
    auto targetPtr = target.Get();

    if (Ptr_.compare_exchange_strong(comparePtr, targetPtr)) {
        target.Release();
        return TIntrusivePtr<T>(comparePtr, false);
    } else {
        compare.Reset();
        compare = THazardPtr<T>::Acquire([&] {
            return Ptr_.load(std::memory_order_relaxed);
        }, comparePtr);
    }

    return TIntrusivePtr<T>();
}

template <class T>
TIntrusivePtr<T> TAtomicPtr<T>::SwapIfCompare(T* comparePtr, TIntrusivePtr<T> target)
{
    auto targetPtr = target.Get();

    static const auto& Logger = LockFreePtrLogger;

    auto savedPtr = comparePtr;
    if (Ptr_.compare_exchange_strong(comparePtr, targetPtr)) {
        YT_LOG_TRACE("CAS succeeded (Compare: %v, Target: %v)",
            comparePtr,
            targetPtr);
        target.Release();
        return TIntrusivePtr<T>(comparePtr, false);
    } else {
        YT_LOG_TRACE("CAS failed (Current: %v, Compare: %v, Target: %v)",
            comparePtr,
            savedPtr,
            targetPtr);
    }

    // TODO(lukyan): Use ptr if compare_exchange_strong fails?
    return TIntrusivePtr<T>();
}

template <class T>
TIntrusivePtr<T> TAtomicPtr<T>::SwapIfCompare(const TIntrusivePtr<T>& compare, TIntrusivePtr<T> target)
{
    return SwapIfCompare(compare.Get(), std::move(target));
}

template <class T>
bool TAtomicPtr<T>::SwapIfCompare(const TIntrusivePtr<T>& compare, TIntrusivePtr<T>* target)
{
     auto ptr = compare.Get();
    if (Ptr_.compare_exchange_strong(ptr, target->Ptr_)) {
        target->Ptr_ = ptr;
        return true;
    }
    return false;
}

template <class T>
TAtomicPtr<T>::operator bool() const
{
    return Ptr_ != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool operator==(const TAtomicPtr<T>& lhs, const TIntrusivePtr<T>& rhs)
{
    return lhs.Ptr_.load() == rhs.Get();
}

template <class T>
bool operator==(const TIntrusivePtr<T>& lhs, const TAtomicPtr<T>& rhs)
{
    return lhs.Get() == rhs.Ptr_.load();
}

template <class T>
bool operator!=(const TAtomicPtr<T>& lhs, const TIntrusivePtr<T>& rhs)
{
    return lhs.Ptr_.load() != rhs.Get();
}

template <class T>
bool operator!=(const TIntrusivePtr<T>& lhs, const TAtomicPtr<T>& rhs)
{
    return lhs.Get() != rhs.Ptr_.load();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
