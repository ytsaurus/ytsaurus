#include "coroutine.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TCoroutineBase::TCoroutineBase()
    : Fiber(New<TFiber>(BIND(&TCoroutineBase::Trampoline, this)))
{ }

TCoroutineBase::~TCoroutineBase()
{ }

void TCoroutineBase::Trampoline()
{
    YUNREACHABLE();
}

EFiberState TCoroutineBase::GetState() const
{
    return Fiber->GetState();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
