#ifndef FLS_INL_H_
#error "Direct inclusion of this file is not allowed, include fls.h"
#endif
#undef FLS_INL_H_

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFls<T>::TFls()
    : Index_(TFiber::FlsAllocateSlot(&ValueCtor, &ValueDtor))
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
T* TFls<T>::Get() const
{
    return GetFor(GetCurrentScheduler()->GetCurrentFiber());
}

template <class T>
T* TFls<T>::GetFor(TFiber* fiber) const
{
    return reinterpret_cast<T*>(fiber->FlsGet(Index_));
}

template <class T>
TFiber::TFlsSlotValue TFls<T>::ValueCtor()
{
    return reinterpret_cast<TFiber::TFlsSlotValue>(new T());
}

template <class T>
void TFls<T>::ValueDtor(TFiber::TFlsSlotValue value)
{
    delete reinterpret_cast<T*>(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
