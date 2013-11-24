#include "stdafx.h"
#include "coroutine.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TCoroutineBase::TCoroutineBase()
    : Fiber(New<TFiber>(BIND(&TCoroutineBase::Trampoline, this)))
{ }

TCoroutineBase::TCoroutineBase(TCoroutineBase&& other)
    : Fiber(std::move(other.Fiber))
{ }

TCoroutineBase::~TCoroutineBase()
{ }

EFiberState TCoroutineBase::GetState() const
{
    return Fiber->GetState();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
