#pragma once
#ifndef FLS_INL_H_
#error "Direct inclusion of this file is not allowed, include fls.h"
#endif
#undef FLS_INL_H_

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFls<T>::TFls()
    : Index_(NDetail::FlsAllocateSlot(&ValueCtor, &ValueDtor))
{ }

template <class T>
T* TFls<T>::operator->()
{
    return Get();
}

template <class T>
const T* TFls<T>::operator->() const
{
    return Get();
}

template <class T>
T& TFls<T>::operator*()
{
    return *Get();
}

template <class T>
const T& TFls<T>::operator*() const
{
    return *Get();
}

template <class T>
T* TFls<T>::Get(TFiber* fiber) const
{
    auto& slot = NDetail::FlsAt(Index_, fiber);
    if (slot == 0) {
        slot = NDetail::FlsConstruct(Index_);
    }
    return reinterpret_cast<T*>(slot);
}

template <class T>
bool TFls<T>::IsInitialized(TFiber* fiber) const
{
    return NDetail::FlsAt(Index_, fiber) != 0;
}

template <class T>
uintptr_t TFls<T>::ValueCtor()
{
    return reinterpret_cast<uintptr_t>(new T());
}

template <class T>
void TFls<T>::ValueDtor(uintptr_t value)
{
    delete reinterpret_cast<T*>(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
