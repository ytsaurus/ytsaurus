#include "stdafx.h"
#include "fiber.h"

#include <contrib/libcoro/coro.h>

#include <stdexcept>

// libcoro asserts that coro_create() is neither thread-safe nor reenterant function.
#ifdef _unix_
#    include <pthread.h>
#    define DEFINE_FIBER_CTOR_MUTEX() \
        static pthread_mutex_t FiberCtorMutex = PTHREAD_MUTEX_INITIALIZER;
#    define BEFORE_FIBER_CTOR() pthread_mutex_lock(&FiberCtorMutex)
#    define AFTER_FIBER_CTOR() pthread_mutex_unlock(&FiberCtorMutex)
#else
#    define DEFINE_FIBER_CTOR_MUTEX()
#    define BEFORE_FIBER_CTOR()
#    define AFTER_FIBER_CTOR()
#endif

#if defined(_unix_) && !defined(CORO_ASM)
#   error "Using slow libcoro backend (expecting CORO_ASM)"
#endif

#if defined(_win_)
#   if !defined(CORO_FIBER)
#       error "Using slow libcoro backend (expecting CORO_FIBER)"
#   endif
#endif

// MSVC compiler has /GT option for supporting fiber-safe thread-local storage.
// For CXXABIv1-compliant systems we can hijack __cxa_eh_globals.
// See http://mentorembedded.github.io/cxx-abi/abi-eh.html
#if defined(__GNUC__) || defined(__clang__)
#   define CXXABIv1

#   ifdef HAVE_CXXABI_H
#       include <cxxabi.h>
#   endif

namespace __cxxabiv1 {
    // We do not care about actual type here, so erase it.
    typedef void __untyped_cxa_exception;
    struct __cxa_eh_globals {
        __untyped_cxa_exception* caughtExceptions;
        unsigned int uncaughtExceptions;
    };
    extern "C" __cxa_eh_globals* __cxa_get_globals() throw();
} // namespace __cxxabiv1

#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Pointer to the current fiber being run by the current thread.
/*!
 *  Current fiber is stored as a raw pointer, all Ref/Unref calls are done manually.
 *  
 *  If |CurrentFiber| is alive (i.e. has positive number of strong references)
 *  then the pointer is owning.
 *  
 *  If |CurrentFiber|s is currently being terminated (i.e. its dtor is in progress)
 *  then the pointer is non-owning.
 * 
 *  Examining |CurrentFiber| could be useful for debugging purposes so we don't
 *  put it into an anonymous namespace to avoid name mangling.
 */
TLS_STATIC TFiber* CurrentFiber = nullptr;

namespace {

DEFINE_FIBER_CTOR_MUTEX();

// Stack sizes are given in machine words.
// Estimates in bytes are given for x86_64.
const size_t SmallFiberStackSize = 1 << 15; // 256 Kb
const size_t LargeFiberStackSize = 1 << 20; //   8 Mb

void InitTls()
{
    if (UNLIKELY(!CurrentFiber)) {
        auto rootFiber = New<TFiber>();
        CurrentFiber = rootFiber.Get();
        CurrentFiber->Ref();
    }
}

} // namespace

class TFiberExceptionHandler
{
public:
    TFiberExceptionHandler()
    {
#ifdef CXXABIv1
        ::memset(&EH, 0, sizeof(EH));
#endif
    }

    void Swap(TFiberExceptionHandler& other)
    {
#ifdef CXXABIv1
        auto* currentEH = __cxxabiv1::__cxa_get_globals();
        YASSERT(currentEH);
        EH = *currentEH;
        *currentEH = other.EH;
#endif
    }

private:
#ifdef CXXABIv1
    __cxxabiv1::__cxa_eh_globals EH;
#endif

};

//} // namespace

////////////////////////////////////////////////////////////////////////////////

class TFiber::TImpl
{
    DEFINE_BYVAL_RO_PROPERTY(EFiberState, State);

public:
    TImpl(TFiber* owner)
        : State_(EFiberState::Running)
        , Owner_(owner)
        , Terminating_(false)
        , Yielded_(false)
    {
        ::memset(&CoroContext_, 0, sizeof(CoroContext_));
        ::memset(&CoroStack_, 0, sizeof(CoroStack_));

        BEFORE_FIBER_CTOR();
        coro_create(&CoroContext_, nullptr, nullptr, nullptr, 0);
        AFTER_FIBER_CTOR();
    }

    TImpl(TFiber* owner, TClosure callee, EFiberStack stack)
        : State_(EFiberState::Initialized)
        , Owner_(owner)
        , Terminating_(false)
        , Yielded_(false)
        , Callee_(std::move(callee))
        , Caller_(nullptr)
    {
        ::memset(&CoroContext_, 0, sizeof(CoroContext_));
        ::memset(&CoroStack_, 0, sizeof(CoroStack_));

        size_t stackSize = GetStackSize(stack);
        coro_stack_alloc(&CoroStack_, stackSize);

        BEFORE_FIBER_CTOR();
        coro_create(
            &CoroContext_,
            &TImpl::Trampoline,
            this,
            CoroStack_.sptr,
            CoroStack_.ssze);
        AFTER_FIBER_CTOR();
    }

    ~TImpl()
    {
        YCHECK(!Caller_);
        YCHECK(Exception_ == std::exception_ptr());

        YCHECK(!Terminating_);
        Terminating_ = true;

        // Root fiber can never be destroyed.
        YCHECK(CoroStack_.sptr && CoroStack_.ssze != 0);

        if (State_ == EFiberState::Suspended) {
            // Most likely that the fiber has been abandoned after being submitted to an invoker.
            // Give the callee the last chance to finish.
            Inject(CreateFiberTerminatedException());
            Run();
        }

        YCHECK(
            State_ == EFiberState::Initialized ||
            State_ == EFiberState::Terminated ||
            State_ == EFiberState::Exception);

        (void) coro_destroy(&CoroContext_);
        (void) coro_stack_free(&CoroStack_);
    }

    
    static TFiber* GetCurrent()
    {
        InitTls();
        return CurrentFiber;
    }


    bool Yielded() const
    {
        return Yielded_;
    }


    void Run()
    {
        YASSERT(!Caller_);
        YCHECK(
            State_ == EFiberState::Initialized ||
            State_ == EFiberState::Suspended);

        Caller_ = TFiber::GetCurrent();

        YCHECK(Caller_->Impl->State_ == EFiberState::Running);
        State_ = EFiberState::Running;

        SetCurrent(Owner_);

        Caller_->Impl->TransferTo(this);

        YCHECK(Caller_->Impl->State_ == EFiberState::Running);

        TFiberPtr caller;
        Caller_.Swap(caller);

        IInvokerPtr switchTo;
        SwitchTo_.Swap(switchTo);

        TFuture<void> waitFor;
        WaitFor_.Swap(waitFor);

        SetCurrent(caller.Get());

        YCHECK(
            State_ == EFiberState::Terminated ||
            State_ == EFiberState::Exception ||
            State_ == EFiberState::Suspended);

        if (State_ == EFiberState::Exception) {
            // Rethrow the propagated exception.

            YCHECK(!Terminating_);
            // XXX(babenko): VS2010 has no operator bool
            YASSERT(!(Exception_ == std::exception_ptr()));

            std::exception_ptr ex;
            std::swap(Exception_, ex);

            std::rethrow_exception(std::move(ex));
        } else if (waitFor) {
            // Schedule wakeup when then given future is set.
            YCHECK(!Terminating_);
            waitFor.Subscribe(BIND(&TFiber::Run, MakeStrong(Owner_)).Via(switchTo));
        } else if (switchTo) {          
            // Schedule switch to another thread.
            YCHECK(!Terminating_);
            switchTo->Invoke(BIND(&TFiber::Run, MakeStrong(Owner_)));
        }
    }

    void Yield()
    {
        // Failure here indicates that the callee has declined our kind offer to exit
        // gracefully and has called Yield once again.
        YCHECK(!Terminating_);

        // Failure here indicates that an attempt is made to Yield control from a root fiber.
        YCHECK(Caller_);

        YCHECK(State_ == EFiberState::Running);
        State_ = EFiberState::Suspended;
        Yielded_ = true;

        TransferTo(Caller_->Impl.get());
        YCHECK(State_ == EFiberState::Running);

        // Rethrow the injected exception, if any.
        // XXX(babenko): VS2010 has no operator bool
        if (!(Exception_ == std::exception_ptr())) {
            std::exception_ptr ex;
            std::swap(Exception_, ex);

            std::rethrow_exception(std::move(ex));
        }
    }


    void Reset()
    {
        YASSERT(!Caller_);
        YASSERT(Exception_ == std::exception_ptr());
        YCHECK(
            State_ == EFiberState::Initialized ||
            State_ == EFiberState::Terminated ||
            State_ == EFiberState::Exception);

        (void) coro_destroy(&CoroContext_);

        BEFORE_FIBER_CTOR();
        coro_create(
            &CoroContext_,
            &TImpl::Trampoline,
            this,
            CoroStack_.sptr,
            CoroStack_.ssze);
        AFTER_FIBER_CTOR();

        State_ = EFiberState::Initialized;
    }

    void Reset(TClosure closure)
    {
        Reset();
        Callee_ = std::move(closure);
    }


    void Inject(std::exception_ptr&& exception)
    {
        // XXX(babenko): VS2010 has no operator bool
        YCHECK(!(exception == std::exception_ptr()));
        YCHECK(
            State_ == EFiberState::Initialized ||
            State_ == EFiberState::Suspended);

        Exception_ = std::move(exception);
    }

    void SwitchTo(IInvokerPtr invoker)
    {
        YCHECK(invoker);
        YCHECK(!WaitFor_);
        YCHECK(!SwitchTo_);

        invoker.Swap(SwitchTo_);

        Yield();
    }

    void WaitFor(TFuture<void> future, IInvokerPtr invoker)
    {
        YCHECK(future);
        YCHECK(invoker);
        YCHECK(!WaitFor_);
        YCHECK(!SwitchTo_);

        future.Swap(WaitFor_);
        invoker.Swap(SwitchTo_);

        Yield();
    }

private:
    TFiber* Owner_;
    bool Terminating_;
    bool Yielded_;

    TClosure Callee_;
    TFiberPtr Caller_;

    coro_context CoroContext_;
    coro_stack CoroStack_;

    std::exception_ptr Exception_;
    TFiberExceptionHandler EH_;

    TFuture<void> WaitFor_;
    IInvokerPtr SwitchTo_;

    static size_t GetStackSize(EFiberStack stack)
    {
        switch (stack)
        {
            case EFiberStack::Small:
                return SmallFiberStackSize;
            case EFiberStack::Large:
                return LargeFiberStackSize;
            default:
                YUNREACHABLE();
        }
    }

    static void SetCurrent(TFiber* fiber)
    {
        InitTls();

        if (CurrentFiber != fiber) {
            if (CurrentFiber && !CurrentFiber->Impl->Terminating_) {
                CurrentFiber->Unref();
            }

            CurrentFiber = fiber;

            if (CurrentFiber && !CurrentFiber->Impl->Terminating_) {
                CurrentFiber->Ref();
            }
        }
    }

    void TransferTo(TImpl* target)
    {
        EH_.Swap(target->EH_);
        coro_transfer(&CoroContext_, &target->CoroContext_);
    }


    static void Trampoline(void* opaque)
    {
        reinterpret_cast<TImpl*>(opaque)->Trampoline();
    }

    void Trampoline()
    {
        YASSERT(Caller_);
        YASSERT(!Callee_.IsNull());

        // XXX(babenko): VS2010 has no operator bool
        if (!(Exception_ == std::exception_ptr())) {
            State_ = EFiberState::Exception;
            TransferTo(Caller_->Impl.get());
            YUNREACHABLE();
        }

        try {
            YCHECK(State_ == EFiberState::Running);

            Callee_.Run();

            YCHECK(State_ == EFiberState::Running);
            State_ = EFiberState::Terminated;
        } catch (const TFiberTerminatedException&) {
            // Thrown intentionally, ignore.
            State_ = EFiberState::Terminated;
        } catch (...) {
            // Failure here indicates that an unhandled exception
            // was thrown during fiber termination.
            YCHECK(!Terminating_);
            Exception_ = std::current_exception();
            State_ = EFiberState::Exception;
        }

        TransferTo(Caller_->Impl.get());
        YUNREACHABLE();
    }

};

////////////////////////////////////////////////////////////////////////////////

TFiber::TFiber()
    : Impl(new TImpl(this))
{ }

TFiber::TFiber(TClosure closure, EFiberStack stack)
    : Impl(new TImpl(this, std::move(closure), stack))
{ }

TFiber::~TFiber()
{ }

TFiber* TFiber::GetCurrent()
{
    return TImpl::GetCurrent();
}

EFiberState TFiber::GetState() const
{
    return Impl->GetState();
}

bool TFiber::Yielded() const
{
    return Impl->Yielded();
}

void TFiber::Run()
{
    Impl->Run();
}

void TFiber::Yield()
{
    Impl->Yield();
}

void TFiber::Reset()
{
    Impl->Reset();
}

void TFiber::Reset(TClosure closure)
{
    Impl->Reset(std::move(closure));
}

void TFiber::Inject(std::exception_ptr&& exception)
{
    Impl->Inject(std::move(exception));
}

void TFiber::SwitchTo(IInvokerPtr invoker)
{
    Impl->SwitchTo(std::move(invoker));
}

void TFiber::WaitFor(TFuture<void> future, IInvokerPtr invoker)
{
    Impl->WaitFor(std::move(future), std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

std::exception_ptr CreateFiberTerminatedException()
{
    try {
        throw TFiberTerminatedException();
    } catch (...) {
        return std::current_exception();
    }
    YUNREACHABLE();
}

void Yield()
{
    TFiber::GetCurrent()->Yield();
}

void WaitFor(TFuture<void> future, IInvokerPtr invoker)
{
    TFiber::GetCurrent()->WaitFor(std::move(future), std::move(invoker));
}

void SwitchTo(IInvokerPtr invoker)
{
    TFiber::GetCurrent()->SwitchTo(std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

}

