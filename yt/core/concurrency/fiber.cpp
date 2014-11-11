#include "stdafx.h"
#include "fiber.h"

#include <core/actions/invoker_util.h>
#include <core/misc/object_pool.h>

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
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

// FLS support.
/*
 * Each registered slot maintains a ctor (to be called when a fiber first accesses
 * the slot) and a dtor (to be called when a fiber terminates).
 *
 * To avoid contention, slot registry has a fixed size (see |MaxFlsSlots|).
 * |FlsGet| and |FlsGet| are lock-free.
 * |FlsRegister|, however, acquires a global lock.
 */

struct TFlsSlot
{
    TFiber::TFlsSlotCtor Ctor;
    TFiber::TFlsSlotDtor Dtor;
};

static const int MaxFlsSlots = 1024;
static TFlsSlot FlsRegistry[MaxFlsSlots] = {};
static TAtomic FlsRegistrySize = 0;
static TSpinLock FlsRegistryLock;

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
        auto rootFiber = NYT::New<TFiber>();
        CurrentFiber = rootFiber.Get();
        CurrentFiber->Ref();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

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

        YCHECK(Base && Base != MAP_FAILED);

        ::mprotect(Base, GetExtraSize(), PROT_NONE);

        Stack = Base + GetExtraSize();
#else
        Base = new char[Size + 15];
        Stack = reinterpret_cast<void*>((reinterpret_cast<uintptr_t>(Base) + 0xF) & ~0xF);
#endif
        YCHECK((reinterpret_cast<ui64>(Stack) & 0xF) == 0);
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

////////////////////////////////////////////////////////////////////////////////

class TFiberContext
{
public:
    TFiberContext()
#ifdef _win_
        : Fiber_(nullptr)
#endif
    { }

    void Reset(void* stack, size_t size, void (*callee)(void*), void* opaque)
    {
#ifdef _win_
        if (Fiber_) {
            DeleteFiber(Fiber_);
        }

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
        if (Fiber_) {
            DeleteFiber(Fiber_);
        }
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
    static void __attribute__((__noinline__, __regparm__(1)))
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
VOID CALLBACK TFiberContext::Trampoline(PVOID opaque)
{
    TFiberContext* context = reinterpret_cast<TFiberContext*>(opaque);
    context->Callee_(context->Opaque_);
}

void TFiberContext::TransferTo(TFiberContext* previous, TFiberContext* next)
{
    if (!previous->Fiber_) {
        previous->Fiber_ = GetCurrentFiber();
        if (previous->Fiber_ == 0 || previous->Fiber_ == (void*)0x1e00) {
            previous->Fiber_ = ConvertThreadToFiber(0);
        }
    }
    SwitchToFiber(next->Fiber_);
}
#endif

////////////////////////////////////////////////////////////////////////////////

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
    std::atomic<int> State_;

public:
    TImpl(TFiber* owner)
        : State_(EFiberState::Running)
        , Owner_(owner)
    {
        Init();
    }

    TImpl(TFiber* owner, TClosure callee, EFiberStack stack)
        : State_(EFiberState::Initialized)
        , Stack_(GetStack(stack))
        , Owner_(owner)
        , Callee_(std::move(callee))
    {
        Init();
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
            State_.load() == EFiberState::Running &&
            !Stack_ &&
            Callee_));

        if (State_.load() == EFiberState::Suspended) {
            // Most likely that the fiber has been abandoned
            // after being submitted to an invoker.
            // Give the callee the last chance to finish.
            Cancel();
        }

        YCHECK(
            State_.load() == EFiberState::Initialized ||
            State_.load() == EFiberState::Terminated ||
            State_.load() == EFiberState::Exception);

        for (int index = 0; index < static_cast<int>(Fls_.size()); ++index) {
            FlsRegistry[index].Dtor(Fls_[index]);
        }
    }

    static TFiber* GetCurrent()
    {
        InitTls();

        return CurrentFiber;
    }

    EFiberState GetState() const
    {
        return EFiberState(State_);
    }

    bool Yielded() const
    {
        return Yielded_;
    }

    bool IsTerminating() const
    {
        return Terminating_;
    }

    bool IsCanceled() const
    {
        return Canceled_;
    }


    void Run()
    {
        YCHECK(
            State_.load() == EFiberState::Initialized ||
            State_.load() == EFiberState::Suspended);

        YCHECK(!Caller_);
        Caller_ = TFiber::GetCurrent();
        if (Caller_ && !Caller_->IsTerminating()) {
            Caller_->Ref();
        }
        SetCurrent(Owner_);

        YCHECK(Caller_->Impl_->State_.load() == EFiberState::Running);
        State_ = EFiberState::Running;

        Caller_->Impl_->TransferTo(this);

        YCHECK(Caller_->Impl_->State_.load() == EFiberState::Running);

        SetCurrent(Caller_);
        if (Caller_ && !Caller_->IsTerminating()) {
            Caller_->Unref();
        }
        Caller_ = nullptr;

        IInvokerPtr switchTo;
        SwitchTo_.Swap(switchTo);

        TFuture<void> waitFor;
        WaitFor_.Swap(waitFor);

        YCHECK(
            State_.load() == EFiberState::Terminated ||
            State_.load() == EFiberState::Exception ||
            State_.load() == EFiberState::Suspended);

        if (State_.load() == EFiberState::Exception) {
            // Rethrow the propagated exception.

            YCHECK(!Canceled_);
            YASSERT(Exception_);

            std::exception_ptr ex;
            std::swap(Exception_, ex);

            std::rethrow_exception(std::move(ex));
        } else if (waitFor) {
            // Schedule wakeup when the given future is set.
            YCHECK(!Canceled_);
            waitFor.Subscribe(BIND(&TImpl::Wakeup, MakeStrong(Owner_)).Via(switchTo));
        } else if (switchTo) {          
            // Schedule switch to another thread.
            YCHECK(!Canceled_);
            switchTo->Invoke(BIND(&TImpl::Wakeup, MakeStrong(Owner_)));
        }
    }

    void Yield()
    {
        // Failure here indicates that the callee has declined our kind offer
        // to exit gracefully and has called |Yield| once again.
        YCHECK(!Canceled_);

        // Failure here indicates that an attempt is made to |Yield| control
        // from a root fiber.
        YCHECK(Caller_);

        YCHECK(State_.load() == EFiberState::Running);
        State_ = EFiberState::Suspended;
        Yielded_ = true;

        TransferTo(Caller_->Impl_.get());
        YCHECK(State_.load() == EFiberState::Running);

        // Throw TFiberTerminatedException if cancellation is requested.
        if (Canceled_) {
            throw TFiberTerminatedException();
        }

        // Rethrow any user injected exception, if any.
        if (Exception_) {
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
            State_.load() == EFiberState::Initialized ||
            State_.load() == EFiberState::Terminated ||
            State_.load() == EFiberState::Exception);

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
        YCHECK(exception);
        YCHECK(
            State_.load() == EFiberState::Initialized ||
            State_.load() == EFiberState::Suspended);

        Exception_ = std::move(exception);
    }

    void Cancel()
    {
        switch (State_.load()) {
            case EFiberState::Initialized:
            case EFiberState::Terminated:
            case EFiberState::Exception:
                break;

            case EFiberState::Suspended:
                Canceled_ = true;
                WaitFor_.Reset();
                SwitchTo_.Reset();
                Exception_ = std::exception_ptr();
                Run();
                break;

            case EFiberState::Running:
                // Failure here indicates that Cancel is called for a fiber
                // that is currently being run in another thread.
                YCHECK(Owner_ == GetCurrent());
                Canceled_ = true;
                throw TFiberTerminatedException();

            default:
                YUNREACHABLE();
        }
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


    static int FlsRegister(TFlsSlotCtor ctor, TFlsSlotDtor dtor)
    {
        TGuard<TSpinLock> guard(FlsRegistryLock);
        YCHECK(FlsRegistrySize < MaxFlsSlots);
        auto& slot = FlsRegistry[FlsRegistrySize];
        slot.Ctor = ctor;
        slot.Dtor = dtor;
        return AtomicIncrement(FlsRegistrySize) - 1;
    }

    TFlsSlotValue FlsGet(int index)
    {
        EnsureFlsSlot(index);
        return Fls_[index];
    }

    void FlsSet(int index, TFlsSlotValue value)
    {
        EnsureFlsSlot(index);
        Fls_[index] = value;
    }

private:
    std::shared_ptr<TFiberStackBase> Stack_;
    TFiberContext Context_;
    TFiberExceptionHandler EH_;

    TFiber* Owner_;
    volatile bool Terminating_;
    bool Canceled_;
    bool Yielded_;

    TClosure Callee_;
    //! Same as for |CurrentFiber|, this reference is owning unless the fiber is terminating.
    TFiber* Caller_;

    std::exception_ptr Exception_;
    TFuture<void> WaitFor_;
    IInvokerPtr SwitchTo_;

    IInvokerPtr CurrentInvoker_;

    std::vector<TFlsSlotValue> Fls_;

    
    void Init()
    {
        Terminating_ = false;
        Canceled_ = false;
        Yielded_ = false;
        Caller_ = nullptr;
        CurrentInvoker_ = GetSyncInvoker();
    }

    void EnsureFlsSlot(int index)
    {
        YASSERT(index >= 0);
        if (index < Fls_.size())
            return;
        int oldSize = static_cast<int>(Fls_.size());
        int newSize = AtomicGet(FlsRegistrySize);
        YCHECK(newSize >= oldSize && index < newSize);
        Fls_.resize(newSize);
        for (int index = oldSize; index < newSize; ++index) {
            Fls_[index] = FlsRegistry[index].Ctor();
        }
    }

    static void Wakeup(TFiberPtr fiber)
    {
        if (fiber->IsCanceled())
            return;

        fiber->Run();
    }

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
            if (CurrentFiber && !CurrentFiber->IsTerminating()) {
                CurrentFiber->Unref();
            }

            CurrentFiber = fiber;

            if (CurrentFiber && !CurrentFiber->IsTerminating()) {
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
        reinterpret_cast<TImpl*>(opaque)->TrampolineImpl();
    }

    void TrampolineImpl()
    {
        YASSERT(Caller_);
        YASSERT(Callee_);

        if (Exception_) {
            State_ = EFiberState::Exception;
        } else if (Canceled_) {
            State_ = EFiberState::Terminated;
        } else {
            try {
                YCHECK(State_.load() == EFiberState::Running);

                Callee_.Run();

                YCHECK(State_.load() == EFiberState::Running);
                State_ = EFiberState::Terminated;
            } catch (const TFiberTerminatedException&) {
                // Thrown intentionally, ignore.
                State_ = EFiberState::Terminated;
            } catch (...) {
                // Failure here indicates that an unhandled exception
                // was thrown during fiber cancellation.
                YCHECK(!Canceled_);
                Exception_ = std::current_exception();
                State_ = EFiberState::Exception;
            }
        }

        // Fall back to the caller.
        TransferTo(Caller_->Impl_.get());
        YUNREACHABLE();
    }

};

////////////////////////////////////////////////////////////////////////////////

TFiber::TFiber()
    : Impl_(new TImpl(this))
{ }

TFiber::TFiber(TClosure closure, EFiberStack stack)
    : Impl_(new TImpl(this, std::move(closure), stack))
{ }

TFiber::~TFiber()
{ }

TFiber* TFiber::GetCurrent()
{
    return TImpl::GetCurrent();
}

EFiberState TFiber::GetState() const
{
    return Impl_->GetState();
}

bool TFiber::Yielded() const
{
    return Impl_->Yielded();
}

bool TFiber::IsTerminating() const
{
    return Impl_->IsTerminating();
}

bool TFiber::IsCanceled() const
{
    return Impl_->IsCanceled();
}

void TFiber::Run()
{
    Impl_->Run();
}

void TFiber::Yield()
{
    Impl_->Yield();
}

void TFiber::Reset()
{
    Impl_->Reset();
}

void TFiber::Reset(TClosure closure)
{
    Impl_->Reset(std::move(closure));
}

void TFiber::Inject(std::exception_ptr&& exception)
{
    Impl_->Inject(std::move(exception));
}

void TFiber::Cancel()
{
    Impl_->Cancel();
}

void TFiber::SwitchTo(IInvokerPtr invoker)
{
    Impl_->SwitchTo(std::move(invoker));
}

void TFiber::WaitFor(TFuture<void> future, IInvokerPtr invoker)
{
    Impl_->WaitFor(std::move(future), std::move(invoker));
}

IInvokerPtr TFiber::GetCurrentInvoker()
{
    return Impl_->GetCurrentInvoker();
}

void TFiber::SetCurrentInvoker(IInvokerPtr invoker)
{
    Impl_->SetCurrentInvoker(std::move(invoker));
}

int TFiber::FlsRegister(TFlsSlotCtor ctor, TFlsSlotDtor dtor)
{
    return TImpl::FlsRegister(ctor, dtor);
}

TFiber::TFlsSlotValue TFiber::FlsGet(int index)
{
    return Impl_->FlsGet(index);
}

void TFiber::FlsSet(int index, TFlsSlotValue value)
{
    return Impl_->FlsSet(index, value);
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

namespace NDetail {

TClosure GetCurrentFiberCanceler()
{
    return BIND(&TFiber::Cancel, MakeStrong(TFiber::GetCurrent()));
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <size_t Size, int GuardedPages>
struct TPooledObjectTraits<
    NConcurrency::TFiberStack<Size, GuardedPages>,
    void>
    : public TPooledObjectTraitsBase
{
    static void Clean(NConcurrency::TFiberStack<Size, GuardedPages>* stack)
    {
#ifndef NDEBUG
        ::memset(stack->GetStack(), 0, stack->GetSize());
#endif
    }

    static int GetMaxPoolSize()
    {
        return 1024;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
