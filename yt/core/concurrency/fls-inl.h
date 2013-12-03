#ifndef FLS_INL_H_
#error "Direct inclusion of this file is not allowed, include fls.h"
#endif
#undef FLS_INL_H_

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFlsValue<T>::TFlsValue()
    : Index_(TFiber::FlsRegister(&ValueCtor, &ValueDtor))
{ }

template <class T>
T* TFlsValue<T>::operator -> ()
{
    return Get();
}

template <class T>
const T* TFlsValue<T>::operator -> () const
{
    return Get();
}

template <class T>
T& TFlsValue<T>::operator * ()
{
    return Get();
}

template <class T>
const T& TFlsValue<T>::operator * () const
{
    return *Get();
}

template <class T>
T* TFlsValue<T>::Get() const
{
    return reinterpret_cast<T*>(TFiber::GetCurrent()->FlsGet(Index_));
}

template <class T>
TFiber::TFlsSlotValue TFlsValue<T>::ValueCtor()
{
    return reinterpret_cast<TFiber::TFlsSlotValue>(new T());
}

template <class T>
void TFlsValue<T>::ValueDtor(TFiber::TFlsSlotValue value)
{
    delete reinterpret_cast<T*>(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
