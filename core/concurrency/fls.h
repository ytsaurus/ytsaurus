#pragma once

#include "public.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail  {

typedef uintptr_t (*TFlsSlotCtor)();
typedef void (*TFlsSlotDtor)(uintptr_t);

int FlsAllocateSlot(TFlsSlotCtor ctor, TFlsSlotDtor dtor);

int FlsCountSlots();

uintptr_t FlsConstruct(int index);
void FlsDestruct(int index, uintptr_t value);

uintptr_t& FlsAt(int index, TFiber* fiber = nullptr);

} // namespace NDetail

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

    T* Get(TFiber* fiber = nullptr) const;

private:
    const int Index_;

    static uintptr_t ValueCtor();
    static void ValueDtor(uintptr_t value);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define FLS_INL_H_
#include "fls-inl.h"
#undef FLS_INL_H_
