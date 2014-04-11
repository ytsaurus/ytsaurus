#include "stdafx.h"
#include "coroutine.h"

namespace NYT {
namespace NConcurrency {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TCoroutineBase::TCoroutineBase()
    : IsCompleted_(false)
    , CoroutineStack_(CreateExecutionStack(EExecutionStack::Small))
    , CoroutineContext_(CreateExecutionContext(*CoroutineStack_, &TCoroutineBase::Trampoline))
{ }

TCoroutineBase::TCoroutineBase(TCoroutineBase&& other)
    : IsCompleted_(other.IsCompleted_)
    , CallerContext_(std::move(other.CallerContext_))
    , CoroutineStack_(std::move(other.CoroutineStack_))
    , CoroutineContext_(std::move(other.CoroutineContext_))
{
    other.IsCompleted_ = true;

    memset(&other.CallerContext_, 0, sizeof(other.CallerContext_));
    memset(&other.CoroutineContext_, 0, sizeof(other.CoroutineContext_));

    other.CoroutineStack_.reset();
}

TCoroutineBase::~TCoroutineBase()
{ }

void TCoroutineBase::Trampoline(void* opaque)
{
    auto* coroutine = reinterpret_cast<TCoroutineBase*>(opaque);
    YASSERT(coroutine);

    try {
        coroutine->Invoke();
    } catch (...) {
        coroutine->CoroutineException_ = std::current_exception();
    }

    coroutine->IsCompleted_ = true;
    coroutine->JumpToCaller();

    YUNREACHABLE();
}

void TCoroutineBase::JumpToCaller()
{
    SwitchExecutionContext(&CoroutineContext_, &CallerContext_, nullptr);
}

void TCoroutineBase::JumpToCoroutine()
{
    SwitchExecutionContext(&CallerContext_, &CoroutineContext_, this);

    if (CoroutineException_) {
        std::exception_ptr exception;
        std::swap(exception, CoroutineException_);
        std::rethrow_exception(std::move(exception));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NConcurrency
} // namespace NYT
