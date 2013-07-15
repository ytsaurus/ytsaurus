#include "stdafx.h"
#include "fiber.h"

#include <ytlib/actions/invoker_util.h>
#include <ytlib/misc/object_pool.h>

#include <stdexcept>

#if defined(_unix_)
#   include <sys/mman.h>
#   include <limits.h>
#   include <unistd.h>
#   if !defined(__x86_64__)
#       error Unsupported platform
#   endif
#endif

#if defined(_win_)
#   define WIN32_LEAN_AND_MEAN
#   if _WIN32_WINNT < 0x0400
#       undef _WIN32_WINNT
#       define _WIN32_WINNT 0x0400
#   endif
#   include <windows.h>
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

const size_t SmallFiberStackSize = 1 << 18; // 256 Kb
const size_t LargeFiberStackSize = 1 << 23; //   8 Mb

static void InitTls()
{
    if (UNLIKELY(!CurrentFiber)) {
        auto rootFiber = New<TFiber>();
        CurrentFiber = rootFiber.Get();
        CurrentFiber->Ref();
    }
}

} // namespace

class TFiberStackBase
{
public:
    TFiberStackBase(char *base, size_t size)
        : Base(base)
        , Size(size)
    { }

    virtual ~TFiberStackBase()
    { }

    void* GetStack() const
    {
        return Stack;
    }

    size_t GetSize() const
    {
        return Size;
    }

protected:
    char* Base;
    void* Stack;
    const size_t Size;
};

template <size_t StackSize, int StackGuardedPages = 4>
class TFiberStack
    : public TFiberStackBase
{
private:
    static const size_t GetExtraSize()
    {
        return GetPageSize() * StackGuardedPages;
    }

public:
    TFiberStack()
        : TFiberStackBase(nullptr, RoundUpToPage(StackSize))
    {
#ifdef _linux_
        Base = reinterpret_cast<char*>(::mmap(
            0,
            Size + GetExtraSize(),
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS,
            -1,
            0));

        if (Base == MAP_FAILED) {
            THROW_ERROR_EXCEPTION("Failed to allocate fiber stack")
                << TErrorAttribute("requested_size", StackSize)
                << TErrorAttribute("allocated_size", Size + GetExtraSize())
                << TErrorAttribute("guarded_pages", StackGuardedPages)
                << TError::FromSystem();
        }

        ::mprotect(Base, GetExtraSize(), PROT_NONE);

        Stack = Base + GetExtraSize();

        YCHECK((reinterpret_cast<ui64>(Stack) & 0xF) == 0);
#else
        Base = new char[Size];
        Stack = Base;

        YCHECK((reinterpret_cast<ui64>(Stack) & 0xF) == 0);
#endif
    }

    ~TFiberStack()
    {
#ifdef _linux_
        ::munmap(Base, Size + GetExtraSize());
#else
        delete[] Base;
#endif
    }

};

template <size_t Size, int GuardedPages>
void CleanPooledObject(TFiberStack<Size, GuardedPages>* stack)
{
#ifndef NDEBUG
    ::memset(stack->GetStack(), 0, stack->GetSize());
#endif
}

class TFiberContext
{
public:
    TFiberContext()
    { }

    void Reset(void* stack, size_t size, void (*callee)(void *), void* opaque)
    {
#ifdef _win_
        DeleteFiber(Fiber_);

        Fiber_ = CreateFiber(size, &TFiberContext::Trampoline, this);
        Callee_ = callee;
        Opaque_ = opaque;
#else
        SP_ = reinterpret_cast<void**>(reinterpret_cast<char*>(stack) + size);

        // We pad an extra nullptr to align %rsp before callq in second trampoline.
        // Effectively, this nullptr mimics a return address.
        *--SP_ = nullptr;
        *--SP_ = (void*) &TFiberContext::Trampoline;
        // See |fiber-supp.s| for precise register mapping.
        *--SP_ = nullptr;        // %rbp
        *--SP_ = (void*) callee; // %rbx
        *--SP_ = (void*) opaque; // %r12
        *--SP_ = nullptr;        // %r13
        *--SP_ = nullptr;        // %r14
        *--SP_ = nullptr;        // %r15
#endif
    }

    ~TFiberContext()
    {
#ifdef _win_
        DeleteFiber(Fiber_);
#endif
    }

    void Swap(TFiberContext& other)
    {
        TransferTo(this, &other);
    }

private:
#ifdef _win_
    void* Fiber_;
    void (*Callee_)(void *);
    void* Opaque_;
#else
    void** SP_;
#endif


#ifdef _win_
    static VOID CALLBACK
    Trampoline(PVOID opaque);
#else
    static void __attribute__((__noinline__))
    Trampoline();
#endif

#ifdef _win_
    static void
    TransferTo(TFiberContext* previous, TFiberContext* next);
#else
    static void __attribute__((__noinline__, __regparm__(2)))
    TransferTo(TFiberContext* previous, TFiberContext* next);
#endif
};

#ifdef _win_
static VOID CALLBACK TFiberContext::Trampoline(PVOID opaque)
{
    TFiberContext* context = reinterpret_cast<TFiberContext*>(opaque);
    context->Callee_(context->Opaque_);
}

static void TFiberContext::TransferTo(TFiberContext* previous, TFiberContext* next)
{
    if (!previous->Fiber_) {
        previous_->Fiber_ = GetCurrentFiber();
        if (previous_->Fiber_ == 0 || previous_->Fiber_ == (void*)0x1e00) {
            previous_->Fiber_ = ConvertThreadToFiber(0);
        }
    }
    SwitchToFiber(next->Fiber_);
}
#endif

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
        , CurrentInvoker_(GetSyncInvoker())
    { }

    TImpl(TFiber* owner, TClosure callee, EFiberStack stack)
        : State_(EFiberState::Initialized)
        , Stack_(GetStack(stack))
        , Owner_(owner)
        , Terminating_(false)
        , Yielded_(false)
        , Callee_(std::move(callee))
        , Caller_(nullptr)
        , CurrentInvoker_(GetSyncInvoker())
    {
        Reset();
    }

    ~TImpl()
    {
        YCHECK(!Caller_);
        YCHECK(Exception_ == std::exception_ptr());

        YCHECK(!Terminating_);
        Terminating_ = true;

        // Root fiber can never be destroyed.
        YCHECK(!(
            State_ == NYT::EFiberState::Running &&
            !Stack_ &&
            !Callee_.IsNull()));

        if (State_ == EFiberState::Suspended) {
            // Most likely that the fiber has been abandoned
            // after being submitted to an invoker.
            // Give the callee the last chance to finish.
            Inject(CreateFiberTerminatedException());
            Run();
        }

        YCHECK(
            State_ == EFiberState::Initialized ||
            State_ == EFiberState::Terminated ||
            State_ == EFiberState::Exception);
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
            // XXX(babenko): VS2010 has no operator bool.
            YASSERT(!(Exception_ == std::exception_ptr()));

            std::exception_ptr ex;
            std::swap(Exception_, ex);

            std::rethrow_exception(std::move(ex));
        } else if (waitFor) {
            // Schedule wakeup when the given future is set.
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
        // Failure here indicates that the callee has declined our kind offer
        // to exit gracefully and has called |Yield| once again.
        YCHECK(!Terminating_);

        // Failure here indicates that an attempt is made to |Yield| control
        // from a root fiber.
        YCHECK(Caller_);

        YCHECK(State_ == EFiberState::Running);
        State_ = EFiberState::Suspended;
        Yielded_ = true;

        TransferTo(Caller_->Impl.get());
        YCHECK(State_ == EFiberState::Running);

        // Rethrow the injected exception, if any.
        // XXX(babenko): VS2010 has no operator bool.
        if (!(Exception_ == std::exception_ptr())) {
            std::exception_ptr ex;
            std::swap(Exception_, ex);

            std::rethrow_exception(std::move(ex));
        }
    }

    void Reset()
    {
        YASSERT(Stack_);
        YASSERT(!Caller_);
        YASSERT(Exception_ == std::exception_ptr());
        YCHECK(
            State_ == EFiberState::Initialized ||
            State_ == EFiberState::Terminated ||
            State_ == EFiberState::Exception);

        Context_.Reset(
            Stack_->GetStack(),
            Stack_->GetSize(),
            &TImpl::Trampoline,
            this);

        State_ = EFiberState::Initialized;
    }

    void Reset(TClosure closure)
    {
        Reset();

        Callee_ = std::move(closure);
    }

    void Inject(std::exception_ptr&& exception)
    {
        // XXX(babenko): VS2010 has no operator bool.
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

        CurrentInvoker_ = invoker;
        SwitchTo_ = std::move(invoker);

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

    IInvokerPtr GetCurrentInvoker()
    {
        return CurrentInvoker_;
    }

    void SetCurrentInvoker(IInvokerPtr invoker)
    {
        CurrentInvoker_ = std::move(invoker);
    }

private:
    std::shared_ptr<TFiberStackBase> Stack_;
    TFiberContext Context_;
    TFiberExceptionHandler EH_;

    TFiber* Owner_;
    bool Terminating_;
    bool Yielded_;

    TClosure Callee_;
    TFiberPtr Caller_;

    std::exception_ptr Exception_;
    TFuture<void> WaitFor_;
    IInvokerPtr SwitchTo_;

    IInvokerPtr CurrentInvoker_;

    static std::shared_ptr<TFiberStackBase> GetStack(EFiberStack stack)
    {
        switch (stack)
        {
            case EFiberStack::Small:
                return ObjectPool<TFiberStack<SmallFiberStackSize>>().Allocate();
            case EFiberStack::Large:
                return ObjectPool<TFiberStack<LargeFiberStackSize>>().Allocate();
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
        Context_.Swap(target->Context_);
    }

#ifdef _linux_
    static void __attribute__((__noinline__, __regparm__(1)))
#else
    static void
#endif
    Trampoline(void* opaque)
    {
        reinterpret_cast<TImpl*>(opaque)->Trampoline();
    }

    void Trampoline()
    {
        YASSERT(Caller_);
        YASSERT(!Callee_.IsNull());

        // XXX(babenko): VS2010 has no operator bool.
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

IInvokerPtr TFiber::GetCurrentInvoker()
{
    return Impl->GetCurrentInvoker();
}

void TFiber::SetCurrentInvoker(IInvokerPtr invoker)
{
    Impl->SetCurrentInvoker(std::move(invoker));
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

