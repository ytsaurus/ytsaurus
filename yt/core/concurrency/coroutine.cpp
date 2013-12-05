#include "stdafx.h"
#include "coroutine.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

NDetail::TCoroutineBase::TCoroutineBase()
    : Fiber(New<TFiber>(BIND(&TCoroutineBase::Trampoline, this)))
{ }

NDetail::TCoroutineBase::~TCoroutineBase()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
