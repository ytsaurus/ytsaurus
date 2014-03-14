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
    extern "C" __cxa_eh_globals* __cxa_get_globals_fast() throw();
} // namespace __cxxabiv1

#endif

namespace NYT {
namespace NConcurrency {

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

//! Pointer to the current executor fiber being run by the current thread.
/*!
 *  This is non-owning reference so no manual Ref/Unref calls are required.
 *
 *  Examining |CurrentExecutor| could be useful for debugging purposes so we don't
 *  put it into an anonymous namespace to avoid name mangling.
 */
TLS_STATIC TFiber* CurrentExecutor = nullptr;

// Fiber-local storage support.
/*
 * Each registered slot maintains a ctor (to be called when a fiber accesses
 * the slot for the first time) and a dtor (to be called when a fiber terminates).
 *
 * To avoid contention, slot registry has a fixed size (see |MaxFlsSlots|).
 * |FlsGet| and |FlsGet| are lock-free.
 *
 * |FlsAllocateSlot|, however, acquires a global lock.
 */
struct TFlsSlot
{
    TFiber::TFlsSlotCtor Ctor;
    TFiber::TFlsSlotDtor Dtor;
};

static const int MaxFlsSlots = 1024;
static TFlsSlot FlsSlots[MaxFlsSlots] = {};
static TAtomic FlsSlotsSize = 0;
static TAtomic FlsSlotsLock = 0;

// Sizes of fiber stacks.
const size_t SmallFiberStackSize = 1 << 18; // 256 Kb
const size_t LargeFiberStackSize = 1 << 23; //   8 Mb

////////////////////////////////////////////////////////////////////////////////

class TFiberStackBase
{
public:
    TFiberStackBase(char* base, size_t size)
        : Base_(base)
        , Size_(size)
    { }

    virtual ~TFiberStackBase()
    { }

    void* GetStack() const
    {
        return Stack_;
    }

    size_t GetSize() const
    {
        return Size_;
    }

protected:
    char* Base_;
    void* Stack_;
    const size_t Size_;

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
        Base_ = reinterpret_cast<char*>(::mmap(
            0,
            GetSize() + GetExtraSize(),
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS,
            -1,
            0));

        if (Base_ == MAP_FAILED) {
            THROW_ERROR_EXCEPTION("Failed to allocate fiber stack")
                << TErrorAttribute("requested_size", StackSize)
                << TErrorAttribute("allocated_size", GetSize() + GetExtraSize())
                << TErrorAttribute("guarded_pages", StackGuardedPages)
                << TError::FromSystem();
        }

        ::mprotect(Base_, GetExtraSize(), PROT_NONE);

        Stack_ = Base_ + GetExtraSize();
#else
        Base_ = new char[Size_ + 15];
        Stack_ = reinterpret_cast<void*>(
            (reinterpret_cast<uintptr_t>(Base_) + 0xF) & ~0xF);
#endif
        // x86_64 requires stack to be 16-byte aligned.
        YCHECK((reinterpret_cast<ui64>(Stack_) & 0xF) == 0);
    }

    ~TFiberStack()
    {
#ifdef _linux_
        ::munmap(Base_, GetSize() + GetExtraSize());
#else
        delete[] Base_;
#endif
    }

};

////////////////////////////////////////////////////////////////////////////////

class TFiberContext
{
public:
    TFiberContext()
#ifdef _win_
        : Handle_(nullptr)
#endif
    { }

#ifdef __clang__
    void Reset(void* stack, size_t size, void __attribute__((__regparm__(1))) (*callee)(void *), void* opaque)
#else
    void Reset(void* stack, size_t size, void (*callee)(void *), void* opaque)
#endif
    {
#ifdef _win_
        if (Handle_) {
            DeleteFiber(Handle_);
        }

        Handle_ = CreateFiber(size, &TFiberContext::Trampoline, this);
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
        if (Handle_) {
            DeleteFiber(Handle_);
        }
#endif
    }

    void Swap(TFiberContext& other)
    {
        TransferTo(this, &other);
    }

private:
#ifdef _win_
    void* Handle_;
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
VOID CALLBACK TFiberContext::Trampoline(PVOID opaque)
{
    auto* context = reinterpret_cast<TFiberContext*>(opaque);
    context->Callee_(context->Opaque_);
}

void TFiberContext::TransferTo(TFiberContext* previous, TFiberContext* next)
{
    if (!previous->Handle_) {
        previous->Handle_ = GetCurrentFiber();
        if (previous->Handle_ == 0 || previous->Handle_ == (void*)0x1e00) {
            previous->Handle_ = ConvertThreadToFiber(0);
        }
    }
    SwitchToFiber(next->Handle_);
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
        auto* currentEH = __cxxabiv1::__cxa_get_globals_fast();
        EH = *currentEH; *currentEH = other.EH;
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
    TImpl(TFiber* this_)
        : State_(EFiberState::Running)
        , This_(this_)
        , Root_(true)
    {
        Init();
    }

    TImpl(TFiber* this_, TClosure callee, EFiberStack stack)
        : State_(EFiberState::Initialized)
        , Stack_(GetStack(stack))
        , This_(this_)
        , Root_(false)
        , Callee_(std::move(callee))
    {
        YCHECK(Stack_);
        Init();
        Reset();
    }

    ~TImpl()
    {
        // Root fiber can never be destroyed in a trivial way.
        YCHECK(!Root_);

        // Running fiber can never be destroyed.
        YCHECK(!Caller_);

        YCHECK(Exception_ == std::exception_ptr());
        YCHECK(!WaitFor_);
        YCHECK(!SwitchTo_);

        YCHECK(!Terminating_.load(std::memory_order_acquire));
        Terminating_.store(true, std::memory_order_release);

        // Give the fiber a chance to cleanup.
        if (State_ == EFiberState::Blocked || State_ == EFiberState::Suspended) {
            Cancel();
        }

        YCHECK(
            State_ == EFiberState::Initialized ||
            State_ == EFiberState::Terminated ||
            State_ == EFiberState::Canceled ||
            State_ == EFiberState::Exception);

        for (int index = 0; index < static_cast<int>(Fls_.size()); ++index) {
            FlsSlots[index].Dtor(Fls_[index]);
        }
    }

    static TFiber* GetCurrent()
    {
        InitTls();
        return CurrentFiber;
    }

    static TFiber* GetExecutor()
    {
        return CurrentExecutor;
    }

    static void SetExecutor(TFiber* executor)
    {
        CurrentExecutor = executor;
    }

    bool HasForked() const
    {
        return Forked_;
    }

    bool IsCanceled() const
    {
        return Canceled_.load(std::memory_order_acquire);
    }

    EFiberState Run()
    {
        auto* caller = GetCurrent();
        YCHECK(caller->Impl_->State_ == EFiberState::Running);
        caller->Impl_->State_ = EFiberState::Blocked;

        if (Canceled_.load(std::memory_order_acquire)) {
            // When fiber is being cancelled control is always transfered
            // to the local closure for cleanup. All descendant fibers
            // will be cancelled automatically.
            YCHECK(
                State_ == EFiberState::Initialized ||
                State_ == EFiberState::Suspended ||
                State_ == EFiberState::Blocked);

            State_ = EFiberState::Running;
            ResumeTo_ = This_;
        } else {
            YCHECK(
                State_ == EFiberState::Initialized ||
                State_ == EFiberState::Suspended);

            YCHECK(!Caller_);
            YCHECK(
                ResumeTo_->Impl_->State_ == EFiberState::Initialized ||
                ResumeTo_->Impl_->State_ == EFiberState::Suspended ||
                ResumeTo_->Impl_->IsCanceled());

            // XXX(sandello): This is temporary fix until fiber rewrite.
            if (ResumeTo_->Impl_->IsCanceled()) {
                throw TFiberCanceledException();
            } else {
                ResumeTo_->Impl_->State_ = EFiberState::Running;
            }

            if (ResumeTo_ != This_) {
                // We are yielding to a suspended descendant
                // hence mark |This_| as blocked again.
                State_ = EFiberState::Blocked;
            }
        }

        SetCurrent(ResumeTo_);
        Caller_ = caller;
        Caller_->Impl_->TransferTo(ResumeTo_->Impl_.get());

        // Acquire a reference to a just yielded fiber.
        auto* yieldedFrom = GetCurrent();
        YASSERT(yieldedFrom);

        YCHECK(Caller_ == caller);

        SetCurrent(Caller_);
        Caller_ = nullptr;

        if (yieldedFrom == caller) {
            // Fiber was interrupted while waiting for a child.
            // This usually means that fiber is canceled.
            YCHECK(caller->Impl_->Canceled_.load(std::memory_order_acquire));
            throw TFiberCanceledException();
        } else {
            // In normal case fiber must receive control back from one of
            // its descendants and continue regular execution.
            YCHECK(caller->Impl_->State_ == EFiberState::Blocked);
            caller->Impl_->State_ = EFiberState::Running;
        }

        // Rescheduling the yielded fiber may pass its ownership to another thread
        // and eventually change its state in case of any races.
        auto state = yieldedFrom->Impl_->State_;
        switch (state) {
            case EFiberState::Terminated:
            case EFiberState::Canceled:
                // Fiber stack must be collapsed in FIFO manner, without jumps.
                YCHECK(yieldedFrom == This_);

                ResumeTo_ = nullptr;

                // Nothing else special to do here.
                break;

            case EFiberState::Exception: {
                // Fiber stack must be collapsed in FIFO manner, without jumps.
                YCHECK(yieldedFrom == This_);

                YCHECK(Exception_);

                // Prepare to rethrow the propagated exception.
                std::exception_ptr exception;
                std::swap(Exception_, exception);

                ResumeTo_ = nullptr;

                // Rethrow.
                std::rethrow_exception(std::move(exception));
                YUNREACHABLE();
            }

            case EFiberState::Suspended: {
                // Fiber stack could be suspended in multiple layers.
                // So the yielder may not be equal to |This_|.
                YCHECK(!Terminating_.load(std::memory_order_acquire));
                YCHECK(!Canceled_.load(std::memory_order_acquire));

                State_ = state;
                ResumeTo_ = yieldedFrom;

                // Before returning control back to the caller fulfill any demands
                // from the yielded fiber concerning its rescheduling.
                IInvokerPtr switchTo;
                yieldedFrom->Impl_->SwitchTo_.Swap(switchTo);

                TFuture<void> waitFor;
                yieldedFrom->Impl_->WaitFor_.Swap(waitFor);

                if (waitFor) {
                    Forked_ = true;
                    // Schedule wakeup when the given future is set.
                    waitFor.Subscribe(BIND(IgnoreResult(&TFiber::Run), MakeStrong(This_)).Via(switchTo));
                } else if (switchTo) {
                    Forked_ = true;
                    // Schedule wakeup when switched to an another thread.
                    switchTo->Invoke(BIND(IgnoreResult(&TFiber::Run), MakeStrong(This_)));
                }

                break;
            }

            default:
                YUNREACHABLE();
        }

        return state;
    }

    void Yield(TFiber* target)
    {
        // Failure here indicates that an attempt is made to |Yield| control
        // from a root fiber.
        YCHECK(!Root_);

        YCHECK(Caller_);
        YCHECK(target);
        YASSERT(DescendsFrom(target)); // This is expensive check.

        YCHECK(State_ == EFiberState::Running);
        if (Canceled_.load(std::memory_order_acquire)) {
            throw TFiberCanceledException();
        }

        State_ = EFiberState::Suspended;
        TransferTo(target->Impl_.get());

        YCHECK(State_ == EFiberState::Running);
        if (Canceled_.load(std::memory_order_acquire)) {
            throw TFiberCanceledException();
        }

        // Rethrow any user injected exception, if any.
        if (Exception_) {
            std::exception_ptr exception;
            std::swap(Exception_, exception);

            std::rethrow_exception(std::move(exception));
        }
    }

    void Yield()
    {
        YCHECK(Caller_);
        Yield(Caller_);
    }

    void Reset()
    {
        YASSERT(Stack_);
        YASSERT(!Terminating_.load(std::memory_order_acquire));
        YASSERT(!Caller_);
        YASSERT(Exception_ == std::exception_ptr());
        YASSERT(!WaitFor_);
        YASSERT(!SwitchTo_);

        YCHECK(
            State_ == EFiberState::Initialized ||
            State_ == EFiberState::Terminated ||
            State_ == EFiberState::Canceled ||
            State_ == EFiberState::Exception);

        Context_.Reset(
            Stack_->GetStack(),
            Stack_->GetSize(),
            &TImpl::Trampoline,
            this);

        State_ = EFiberState::Initialized;
        Canceled_.store(false, std::memory_order_release);
        Forked_ = false;
        ResumeTo_ = This_;
        CurrentInvoker_ = GetSyncInvoker();
    }

    void Reset(TClosure closure)
    {
        Reset();

        Callee_ = std::move(closure);
    }

    void Inject(std::exception_ptr&& exception)
    {
        YCHECK(
            State_ == EFiberState::Initialized ||
            State_ == EFiberState::Blocked ||
            State_ == EFiberState::Suspended);
        YCHECK(
            State_ != EFiberState::Blocked || (
                ResumeTo_ != This_ &&
                ResumeTo_->Impl_->State_ == EFiberState::Suspended));
        YCHECK(!Caller_);
        YCHECK(exception);

        ResumeTo_->Impl_->Exception_ = std::move(exception);
    }

    void Cancel()
    {
        switch (State_) {
            case EFiberState::Initialized:
            case EFiberState::Terminated:
            case EFiberState::Canceled:
            case EFiberState::Exception:
                Canceled_.store(true, std::memory_order_release);
                break;

            case EFiberState::Running:
                Canceled_.store(true, std::memory_order_release);
                // If this is a current fiber then we can unwind it.
                if (GetCurrent() == This_) {
                    throw TFiberCanceledException();
                }
                break;

            case EFiberState::Blocked:
            case EFiberState::Suspended:
                Canceled_.store(true, std::memory_order_release);
                Run();
                break;

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
        SwitchTo_.Swap(invoker);

        auto* executor = GetExecutor();
        YCHECK(executor);
        Yield(executor);
    }

    void WaitFor(TFuture<void> future, IInvokerPtr invoker)
    {
        if (future.IsSet())
            return; // shortcut

        YCHECK(future);
        YCHECK(invoker);
        YCHECK(!WaitFor_);
        YCHECK(!SwitchTo_);

        WaitFor_.Swap(future);
        SwitchTo_.Swap(invoker);

        auto* executor = GetExecutor();
        YCHECK(executor);
        Yield(executor);
    }

    IInvokerPtr GetCurrentInvoker() const
    {
        return CurrentInvoker_;
    }

    void SetCurrentInvoker(IInvokerPtr invoker)
    {
        CurrentInvoker_ = std::move(invoker);
    }

    static int FlsAllocateSlot(TFlsSlotCtor ctor, TFlsSlotDtor dtor)
    {
        AcquireSpinLock(&FlsSlotsLock);
        int result = AtomicGet(FlsSlotsSize);
        YCHECK(result < MaxFlsSlots);
        auto& slot = FlsSlots[result];
        slot.Ctor = ctor;
        slot.Dtor = dtor;
        AtomicIncrement(FlsSlotsSize);
        ReleaseSpinLock(&FlsSlotsLock);
        return result;
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
    friend class TFiber;

    TFiberContext Context_;
    TFiberExceptionHandler EH_;
    std::shared_ptr<TFiberStackBase> Stack_;

    TFiber* const This_;
    bool const Root_;

    std::atomic<bool> Terminating_;
    std::atomic<bool> Canceled_;

    bool Forked_;

    TClosure Callee_;
    //! Those reference is owning unless the fiber is terminating
    //! (same as for |CurrentFiber|).
    TFiber* Caller_;
    TFiber* ResumeTo_;

    std::exception_ptr Exception_;
    TFuture<void> WaitFor_;
    IInvokerPtr SwitchTo_;

    IInvokerPtr CurrentInvoker_;

    std::vector<TFlsSlotValue> Fls_;

    void EnsureFlsSlot(int index)
    {
        YASSERT(index >= 0);
        if (index < Fls_.size()) {
            return;
        }

        int oldSize = static_cast<int>(Fls_.size());
        int newSize = FlsSlotsSize;

        YCHECK(newSize >= oldSize && index < newSize);
        Fls_.resize(newSize);

        for (int index = oldSize; index < newSize; ++index) {
            Fls_[index] = FlsSlots[index].Ctor();
        }
    }

    static std::shared_ptr<TFiberStackBase> GetStack(EFiberStack stack)
    {
        switch (stack) {
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
        YASSERT(fiber);

        if (CurrentFiber != fiber) {
            if (!CurrentFiber->Impl_->Terminating_.load(std::memory_order_acquire)) {
                CurrentFiber->Unref();
            }

            CurrentFiber = fiber;

            if (!CurrentFiber->Impl_->Terminating_.load(std::memory_order_acquire)) {
                CurrentFiber->Ref();
            }
        }
    }

    void Init()
    {
        Terminating_.store(false, std::memory_order_release);
        Canceled_.store(false, std::memory_order_release);
        Forked_ = false;
        Caller_ = nullptr;
        ResumeTo_ = This_;
        CurrentInvoker_ = GetSyncInvoker();
    }

    void TransferTo(TImpl* target)
    {
        EH_.Swap(target->EH_);
        Context_.Swap(target->Context_);
    }

    bool DescendsFrom(TFiber* caller)
    {
        auto* current = Caller_;
        while (current) {
            if (current == caller) {
                return true;
            }
            current = current->Impl_->Caller_;
        }
        return false;
    }

#ifdef _unix_
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
        YASSERT(Callee_);

        if (Exception_) {
            State_ = EFiberState::Exception;
        } else if (Canceled_.load(std::memory_order_acquire)) {
            State_ = EFiberState::Canceled;
        } else {
            try {
                YCHECK(State_ == EFiberState::Running);
                Callee_.Run();
                YCHECK(State_ == EFiberState::Running);
                State_ = EFiberState::Terminated;
            } catch (const TFiberCanceledException&) {
                // Thrown intentionally, ignore.
                State_ = EFiberState::Canceled;
            } catch (...) {
                // Failure here indicates that an unhandled exception
                // was thrown during fiber cancellation.
                YCHECK(!Canceled_.load(std::memory_order_acquire));

                Exception_ = std::current_exception();
                State_ = EFiberState::Exception;
            }
        }

        // Jump back to the caller.
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

void TFiber::InitTls()
{
    if (UNLIKELY(!CurrentFiber)) {
        auto fiber = New<TFiber>();
        CurrentFiber = fiber.Get();
        CurrentFiber->Ref(); // This is to-be-dropped reference from |fiber|.
        CurrentFiber->Ref(); // This is an extra "magic" reference.
        // TODO(sandello): Move me out of here.
        CurrentExecutor = CurrentFiber;
#ifdef CXXABIv1
        __cxxabiv1::__cxa_get_globals();
#endif
    }
}

void TFiber::FiniTls()
{
    auto* current = CurrentFiber;
    if (!current) return;
    YCHECK(current->Impl_->Root_);
    YCHECK(current->GetRefCount() == 2);
    YCHECK(current->GetState() == EFiberState::Running);
    // Transition to destroyable state.
    *const_cast<bool*>(&current->Impl_->Root_) = false;
    current->Impl_->State_ = EFiberState::Terminated;
    current->Unref();
    current->Unref();
    CurrentFiber = nullptr;
}

TFiber* TFiber::GetCurrent()
{
    return TImpl::GetCurrent();
}

TFiber* TFiber::GetExecutor()
{
    return TImpl::GetExecutor();
}

void TFiber::SetExecutor(TFiber* executor)
{
    return TImpl::SetExecutor(executor);
}

int TFiber::FlsAllocateSlot(TFlsSlotCtor ctor, TFlsSlotDtor dtor)
{
    return TImpl::FlsAllocateSlot(ctor, dtor);
}

TFiber::TFlsSlotValue TFiber::FlsGet(int index)
{
    return Impl_->FlsGet(index);
}

void TFiber::FlsSet(int index, TFlsSlotValue value)
{
    return Impl_->FlsSet(index, value);
}

EFiberState TFiber::GetState() const
{
    return Impl_->GetState();
}

bool TFiber::HasForked() const
{
    return Impl_->HasForked();
}

bool TFiber::IsCanceled() const
{
    return Impl_->IsCanceled();
}

EFiberState TFiber::Run()
{
    return Impl_->Run();
}

void TFiber::Yield()
{
    Impl_->Yield();
}

void TFiber::Yield(TFiber* caller)
{
    Impl_->Yield(caller);
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

IInvokerPtr TFiber::GetCurrentInvoker() const
{
    return Impl_->GetCurrentInvoker();
}

void TFiber::SetCurrentInvoker(IInvokerPtr invoker)
{
    Impl_->SetCurrentInvoker(std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

void Yield(TFiber* target)
{
    if (target == nullptr) {
        TFiber::GetCurrent()->Yield();
    } else {
        TFiber::GetCurrent()->Yield(target);
    }
}

void SwitchTo(IInvokerPtr invoker)
{
    TFiber::GetCurrent()->Impl_->SwitchTo(std::move(invoker));
}

void WaitFor(TFuture<void> future, IInvokerPtr invoker)
{
    TFiber::GetCurrent()->Impl_->WaitFor(std::move(future), std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
TClosure GetCurrentFiberCanceler()
{
    return BIND(&TFiber::Cancel, MakeStrong(TFiber::GetCurrent()));
}
} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <size_t StackSize, int StackGuardedPages>
struct TPooledObjectTraits<
    NConcurrency::TFiberStack<StackSize, StackGuardedPages>,
    void>
    : public TPooledObjectTraitsBase
{
    static void Clean(NConcurrency::TFiberStack<StackSize, StackGuardedPages>* stack)
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
