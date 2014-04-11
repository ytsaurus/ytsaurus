#pragma once

#include "public.h"
#include "fiber.h"
#include "scheduler.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TFls
{
public:
    TFls();

    const T& operator*() const;
    T& operator*();

    const T* operator->() const;
    T* operator->();

    T* Get() const;
    T* GetFor(TFiber* fiber) const;

private:
    const int Index_;

    static TFiber::TFlsSlotValue ValueCtor();
    static void ValueDtor(TFiber::TFlsSlotValue value);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define FLS_INL_H_
#include "fls-inl.h"
#undef FLS_INL_H_
