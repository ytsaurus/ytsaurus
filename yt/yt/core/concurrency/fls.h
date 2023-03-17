#pragma once

#include "public.h"

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

using TFlsSlotCtor = uintptr_t(*)();
using TFlsSlotDtor = void(*)(uintptr_t);

int FlsAllocateSlot(TFlsSlotDtor dtor);

int FlsCountSlots();

uintptr_t FlsConstruct(TFlsSlotCtor ctor);
void FlsDestruct(int index, uintptr_t value);

uintptr_t& FlsAt(int index);

class TFsdHolder
{
public:
    uintptr_t& FsdAt(int index);

    ~TFsdHolder();

private:
    TCompactVector<uintptr_t, 8> Fsd_;

    void FsdResize();
};

TFsdHolder* SetCurrentFsdHolder(TFsdHolder* currentFsd);

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

    T* Get() const;

    bool IsInitialized() const;

private:
    const int Index_;

    static uintptr_t ValueCtor();
    static void ValueDtor(uintptr_t value);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define FLS_INL_H_
#include "fls-inl.h"
#undef FLS_INL_H_
