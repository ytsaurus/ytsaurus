#pragma once

#include "public.h"
#include "fiber.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TFlsValue
{
public:
    TFlsValue();

    const T& operator * () const;
    T& operator * ();

    const T* operator -> () const;
    T* operator -> ();

private:
    int Index_;


    T* Get() const;

    static TFiber::TFlsSlotValue ValueCtor();
    static void ValueDtor(TFiber::TFlsSlotValue value);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define FLS_INL_H_
#include "fls-inl.h"
#undef FLS_INL_H_
