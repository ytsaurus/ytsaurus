#pragma once
#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TNonNullPtrBase<T>::TNonNullPtrBase(T* ptr) noexcept
    : Ptr_(ptr)
{
    YT_VERIFY(ptr);
}

template <class T>
T* TNonNullPtrBase<T>::operator->() const noexcept
{
    return Ptr_;
}

template <class T>
T& TNonNullPtrBase<T>::operator*() const noexcept
{
    return *Ptr_;
}

template <class T>
TNonNullPtrBase<T>::TNonNullPtrBase() noexcept
    : Ptr_(nullptr)
{ }

template <class T>
TNonNullPtr<T> GetPtr(T& ref) noexcept
{
    TNonNullPtr<T> ptr;
    ptr.Ptr_ = &ref;

    return ptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
