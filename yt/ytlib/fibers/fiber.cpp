#include "stdafx.h"
#include "fiber.h"

// libcoro asserts that coro_create() is not thread-safe nor reenterant function.
#ifdef _unix_
    #include <pthread.h>
    #define DEFINE_FIBER_CTOR_MUTEX() \
        static pthread_mutex_t FiberCtorMutex = PTHREAD_MUTEX_INITIALIZER;
    #define BEFORE_FIBER_CTOR() pthread_mutex_lock(&FiberCtorMutex)
    #define AFTER_FIBER_CTOR() pthread_mutex_unlock(&FiberCtorMutex)
#else
    #define DEFINE_FIBER_CTOR_MUTEX()
    #define BEFORE_FIBER_CTOR()
    #define AFTER_FIBER_CTOR()
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {
    // Stack sizes are given in machine words.
    // Estimates in bytes are given for x86_64.
    static const size_t SmallFiberStackSize = 1 << 12; // 32K
    static const size_t LargeFiberStackSize = 1 << 20; // 8MB
    TFiberPtr FiberCurrent;
    TFiberPtr FiberMain;

    DEFINE_FIBER_CTOR_MUTEX();
} // namespace

TFiberExceptionHandler::TFiberExceptionHandler()
{
#ifdef CXXABIv1
    ::memset(&EH, 0, sizeof(EH));
#endif
}

TFiberExceptionHandler::~TFiberExceptionHandler()
{ }

void TFiberExceptionHandler::Swap(TFiberExceptionHandler& other)
{
#ifdef CXXABIv1
    auto* currentEH = __cxxabiv1::__cxa_get_globals();
    YASSERT(currentEH);
    EH = *currentEH;
    *currentEH = other.EH;
#endif
}

TFiber::TFiber()
    : State_(EFiberState::Running)
{
    ::memset(&CoroContext, 0, sizeof(CoroContext));
    ::memset(&CoroStack, 0, sizeof(CoroStack));

    BEFORE_FIBER_CTOR();
    coro_create(&CoroContext, nullptr, nullptr, nullptr, 0);
    AFTER_FIBER_CTOR();
}

TFiber::TFiber(TClosure closure, EFiberStack stack)
    : State_(EFiberState::Initialized)
    , Callee(std::move(closure))
    , Caller(nullptr)
{
    ::memset(&CoroContext, 0, sizeof(CoroContext));
    ::memset(&CoroStack, 0, sizeof(CoroStack));

    size_t stackSize = 0;
    switch (stack)
    {
        case EFiberStack::Small:
            stackSize = SmallFiberStackSize;
            break;
        case EFiberStack::Large:
            stackSize = LargeFiberStackSize;
            break;
        default:
            YUNREACHABLE();
    }

    coro_stack_alloc(&CoroStack, stackSize);

    BEFORE_FIBER_CTOR();
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
    YCHECK(!Caller);
    YCHECK(!(Exception == std::exception_ptr()));

    if (LIKELY(CoroStack.sptr != nullptr && CoroStack.ssze != 0)) {
        // This is a spawned fiber.
        YCHECK(
            State_ == EFiberState::Initialized ||
            State_ == EFiberState::Terminated ||
            State_ == EFiberState::Exception);

        (void)coro_destroy(&CoroContext);
        (void)coro_stack_free(&CoroStack);
    } else {
        // This is a main fiber.
        YCHECK(State_ == EFiberState::Running);

#ifdef CORO_ASM
        YASSERT(CoroContext.sp == nullptr);
#endif
#ifdef CORO_FIBER
        YASSERT(CoroContext.fiber == nullptr);
#endif
    }
}

TFiberPtr TFiber::GetCurrent()
{
    if (!FiberCurrent) {
        FiberCurrent = New<TFiber>();
    }
    return FiberCurrent;
}

void TFiber::SetCurrent(const TFiberPtr& fiber)
{
    FiberCurrent = fiber;
}

void TFiber::SetCurrent(TFiberPtr&& fiber)
{
    FiberCurrent = std::move(fiber);
}

void TFiber::Yield()
{
    auto current = TFiber::GetCurrent();
    YASSERT(current);
    YASSERT(current->Caller);

    YCHECK(current->State_ == EFiberState::Running);
    current->State_ = EFiberState::Suspended;
    current->SwitchTo(current->Caller.Get());
    YCHECK(current->State_ == EFiberState::Running);

    // If we have an injected exception, rethrow it.
#ifdef _win_
    if (!(current->Exception == std::exception_ptr())) {
#else
    if (current->Exception) {
#endif
        // For some reason, std::move does not nullify the moved pointer.
        auto ex = std::move(current->Exception);
        current->Exception = nullptr;
        std::rethrow_exception(std::move(ex));
    }
}

void TFiber::Run()
{
    YASSERT(!Caller);
    YCHECK(
        State_ == EFiberState::Initialized ||
        State_ == EFiberState::Suspended);

    TFiberPtr caller = TFiber::GetCurrent();
    TFiber* rawCaller = caller.Get();

    YASSERT(rawCaller->State_ == EFiberState::Running);
    State_ = EFiberState::Running;
    Caller = std::move(caller);
    TFiber::SetCurrent(this);
    Caller->SwitchTo(this);
    TFiber::SetCurrent(std::move(Caller));
    YASSERT(rawCaller->State_ == EFiberState::Running);

    YASSERT(!Caller);
    YCHECK(
        State_ == EFiberState::Terminated ||
        State_ == EFiberState::Exception ||
        State_ == EFiberState::Suspended);

    // If we have a propagated exception, rethrow it.
    if (State_ == EFiberState::Exception) {
        // Ensure that there is an exception object.
        YASSERT(!(Exception == std::exception_ptr()));
        // For some reason, std::move does not nullify the moved pointer.
        auto ex = std::move(Exception);
        Exception = nullptr;
        std::rethrow_exception(std::move(ex));
    }
}

void TFiber::Reset()
{
    YASSERT(!Caller);
    YASSERT(!(Exception == std::exception_ptr()));
    YCHECK(
        State_ == EFiberState::Initialized ||
        State_ == EFiberState::Terminated ||
        State_ == EFiberState::Exception);

    (void)coro_destroy(&CoroContext);

    BEFORE_FIBER_CTOR();
    coro_create(
        &CoroContext,
        &TFiber::Trampoline,
        this,
        CoroStack.sptr,
        CoroStack.ssze);
    AFTER_FIBER_CTOR();

    State_ = EFiberState::Initialized;
}

void TFiber::Reset(TClosure closure)
{
    Reset();
    Callee = std::move(closure);
}

void TFiber::Inject(std::exception_ptr&& exception)
{
    YCHECK(
        State_ == EFiberState::Initialized ||
        State_ == EFiberState::Suspended);

    Exception = std::move(exception);
}

void TFiber::SwitchTo(TFiber* target)
{
    coro_transfer(&CoroContext, &target->CoroContext);
    EH.Swap(target->EH);
}

void TFiber::Trampoline(void* opaque)
{
    TFiber* fiber = reinterpret_cast<TFiber*>(opaque);
    YASSERT(fiber);
    YASSERT(fiber->Caller);
    YASSERT(!fiber->Callee.IsNull());

#ifdef _win_
    if (!(fiber->Exception == std::exception_ptr())) {
#else
    if (fiber->Exception) {
#endif
        fiber->State_ = EFiberState::Exception;
        fiber->SwitchTo(fiber->Caller.Get());
        YUNREACHABLE();
    }

    try {
        YCHECK(fiber->State_ == EFiberState::Running);
        fiber->Callee.Run();
        YCHECK(fiber->State_ == EFiberState::Running);
        fiber->State_ = EFiberState::Terminated;
    } catch (...) {
        fiber->Exception = std::current_exception();
        fiber->State_ = EFiberState::Exception;
    }

    fiber->SwitchTo(fiber->Caller.Get());
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

}

