#include "stdafx.h"
#include "fiber.h"

#ifdef _unix_
#include <pthread.h>
#define DEFINE_FIBER_CTOR_MUTEX() \
    static pthread_mutex_t FiberCtorMutex = PTHREAD_MUTEX_INITIALIZER;
#define BEFORE_FIBER_CTOR() pthread_mutex_lock(&FiberCtorMutex)
#define AFTER_FIBER_CTOR() pthread_mutex_unlock(&FiberCtorMutex)
#endif

namespace NYT {

namespace {
    // Stack sizes are given in machine words.
    // Estimates in bytes are given for x86_64.
    static const size_t SmallFiberStackSize = 1 << 12; // 32K
    static const size_t LargeFiberStackSize = 1 << 20; // 8MB
    TFiberPtr FiberCurrent;
    TFiberPtr FiberMain;

    DEFINE_FIBER_CTOR_MUTEX();
}

TFiber::TFiber()
    : State_(EState::Running)
{
    ::memset(&CoroContext, 0, sizeof(CoroContext));
    ::memset(&CoroStack, 0, sizeof(CoroStack));

    BEFORE_FIBER_CTOR();
    coro_create(&CoroContext, NULL, NULL, NULL, 0);
    AFTER_FIBER_CTOR();
}

TFiber::TFiber(TClosure closure, EStack stack)
    : State_(EState::Initialized)
    , Callee(std::move(closure))
    , Caller(nullptr)
{
    ::memset(&CoroContext, 0, sizeof(CoroContext));
    ::memset(&CoroStack, 0, sizeof(CoroStack));

    size_t stackSize = 0;
    switch (stack)
    {
        case SmallStack:
            stackSize = SmallFiberStackSize;
            break;
        case LargeStack:
            stackSize = LargeFiberStackSize;
            break;
    }

    BEFORE_FIBER_CTOR();
    coro_stack_alloc(&CoroStack, stackSize);
    coro_create(
        &CoroContext,
        &TFiber::Trampoline,
        this,
        CoroStack.sptr,
        CoroStack.ssze);
    AFTER_FIBER_CTOR();
}

TFiber::~TFiber()
{
    (void)coro_destroy(&CoroContext);
    (void)coro_stack_free(&CoroStack);
}

TFiberPtr TFiber::GetCurrent()
{
    if (!FiberCurrent) {
        FiberCurrent = New<TFiber>();
    }
    return FiberCurrent;
}

void TFiber::SetCurrent(TFiberPtr fiber)
{
    FiberCurrent = std::move(fiber);
}

void TFiber::Yield()
{
    auto current = TFiber::GetCurrent();
    YASSERT(current);
    YASSERT(current->Caller);

    YCHECK(current->State_ == EState::Running);
    current->State_ = EState::Suspended;
    coro_transfer(&current->CoroContext, &current->Caller->CoroContext);
    YCHECK(current->State_ == EState::Running);
}

void TFiber::Run()
{
    YASSERT(!Caller);

    TFiber* rawCaller = nullptr;
    YCHECK(State_ == EState::Initialized || State_ == EState::Suspended);
    State_ = EState::Running;
    Caller = TFiber::GetCurrent();
    rawCaller = Caller.Get();
    YCHECK(rawCaller->State_ == EState::Running);
    TFiber::SetCurrent(this);
    Caller->SwitchTo(*this);
    TFiber::SetCurrent(std::move(Caller));
    YCHECK(State_ == EState::Suspended || State_ == EState::Terminated);
    YCHECK(rawCaller->State_ == EState::Running);
}

void TFiber::Reset(TClosure closure)
{
    YCHECK(!Caller);
    YCHECK(State_ == EState::Initialized);

    Callee = std::move(closure);
    State_ = EState::Initialized;
}

void TFiber::Inject(std::exception_ptr ex)
{
    YUNIMPLEMENTED();
}

void TFiber::SwitchTo(TFiber& target)
{
    coro_transfer(&CoroContext, &target.CoroContext);
}

void TFiber::SwitchTo(const TFiberPtr& target)
{
    coro_transfer(&CoroContext, &target->CoroContext);
}

void TFiber::Trampoline(void* arg)
{
    TFiber* fiber = reinterpret_cast<TFiber*>(arg);
    YASSERT(fiber);
    YASSERT(fiber->Caller);
    YASSERT(!fiber->Callee.IsNull());

    try {
        YASSERT(fiber->State_ == EState::Running);
        fiber->Callee.Run();
        YASSERT(fiber->State_ == EState::Running);
    } catch (const std::exception& ex) {
        fprintf(
            stderr,
            "*** Uncaught exception in TFiber: %s\n",
            ex.what());
        abort();
    }

    fiber->State_ = EState::Terminated;
    fiber->SwitchTo(fiber->Caller.Get());
    YUNREACHABLE();
}

}

