#pragma once

#include "public.h"

#include <yt/core/misc/small_vector.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

typedef uintptr_t (*TFlsSlotCtor)();
typedef void (*TFlsSlotDtor)(uintptr_t);

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
    SmallVector<uintptr_t, 8> Fsd_;

    void FsdResize();
};

void SetCurrentFsdHolder(TFsdHolder* currentFsd);

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
