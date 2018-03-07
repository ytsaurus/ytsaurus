#pragma once
#ifndef ATOMIC_OBJECT_INL_H_
#error "Direct inclusion of this file is not allowed, include atomic_object.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TAtomicObject<T>::TAtomicObject(const T& other)
    : Object_(other.Load())
{ }

template <class T>
template <class U>
void TAtomicObject<T>::Store(U&& u)
{
    NConcurrency::TWriterGuard guard(Spinlock_);
    Object_ = std::forward<U>(u);
}

template <class T>
T TAtomicObject<T>::Load() const
{
    NConcurrency::TReaderGuard guard(Spinlock_);
    return Object_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
