#include "stdafx.h"
#include "coroutine.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

NDetail::TCoroutineBase::TCoroutineBase()
    : Fiber_(New<TFiber>(BIND(&TCoroutineBase::Trampoline, this)))
{ }

NDetail::TCoroutineBase::TCoroutineBase(TCoroutineBase&& other)
    : Fiber_(std::move(other.Fiber_))
{ }

NDetail::TCoroutineBase::~TCoroutineBase()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
