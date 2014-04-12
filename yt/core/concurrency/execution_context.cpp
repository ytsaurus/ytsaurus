#include "stdafx.h"
#include "execution_context.h"
#include "execution_stack.h"

#if defined(_win_)
#   define WIN32_LEAN_AND_MEAN
#   if _WIN32_WINNT < 0x0400
#       undef _WIN32_WINNT
#       define _WIN32_WINNT 0x0400
#   endif
#   include <windows.h>
#endif

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

#if defined(_unix_)

extern "C" void* __attribute__((__regparm__(3))) SwitchExecutionContextImpl(
    void** caller,
    void** target,
    void* opaque);

TExecutionContext::TExecutionContext()
    : SP(nullptr)
{
    memset(&EH, 0, sizeof(EH));
}

TExecutionContext::TExecutionContext(TExecutionContext&& other)
{
    SP = other.SP;
    other.SP = nullptr;

#ifdef CXXABIv1
    EH = other.EH;
    memset(&other.EH, 0, sizeof(other.EH));
#endif
}

TExecutionContext CreateExecutionContext(
    TExecutionStack* stack,
    TTrampoline trampoline)
{
    TExecutionContext context;
    memset(&context, 0, sizeof(context));

    auto* sp = reinterpret_cast<void**>(reinterpret_cast<char*>(stack->GetStack()) + stack->GetSize());
    // We pad an extra nullptr to align %rsp before callq after jmpq.
    // Effectively, this nullptr mimics a return address.
    *--sp = nullptr;
    *--sp = reinterpret_cast<void*>(trampoline);
    // No need to set any extra registers, so just pad for them.
    sp -= 6;
    context.SP = sp;

    return context;
}

void* SwitchExecutionContext(
    TExecutionContext* caller,
    TExecutionContext* target,
    void* opaque)
{
#ifdef CXXABIv1
    auto* eh = __cxxabiv1::__cxa_get_globals_fast();
    caller->EH = *eh;
    *eh = target->EH;
#endif
    return SwitchExecutionContextImpl(&caller->SP, &target->SP, opaque);
}

#elif defined(_win_)

static VOID CALLBACK FiberTrampoline(PVOID opaque)
{
    auto* contextImpl = reinterpret_cast<TExecutionContextImpl*>(opaque);
    contextImpl->Callee(contextImpl->Opaque);
}

TExecutionContext::TExecutionContext()
    : Impl(std::make_unique<TExecutionContextImpl>())
{  }

TExecutionContextImpl::TExecutionContextImpl()
    : Owning(false)
    , Handle(nullptr)
    , Opaque(nullptr)
    , Callee(nullptr)
{ }

TExecutionContextImpl::TExecutionContextImpl(TExecutionContextImpl&& other)
{
    Owning = other.Owning;
    other.Owning = false;

    Handle = other.Handle;
    other.Handle = nullptr;

    Opaque = other.Opaque;
    other.Opaque = nullptr;

    Callee = other.Callee;
    other.Callee = nullptr;
}

TExecutionContextImpl::~TExecutionContextImpl()
{
    if (Owning) {
        DeleteFiber(Handle);
    }
}

TExecutionContext CreateExecutionContext(
    TExecutionStack* stack,
    TTrampoline trampoline)
{
    TExecutionContext context;
    context.Impl->Owning = true;
    context.Impl->Handle = CreateFiber(stack->GetSize(), &FiberTrampoline, context.Impl.get());
    context.Impl->Callee = trampoline;
    return context;
}

void* SwitchExecutionContext(
    TExecutionContext* caller,
    TExecutionContext* target,
    void* opaque)
{
    if (!caller->Impl->Handle) {
        auto callerFiber = GetCurrentFiber();
        if (callerFiber == (void*)0x0 || callerFiber == (void*)0x1e00) {
            callerFiber = ConvertThreadToFiber(0);
        }
        caller->Impl->Owning = false;
        caller->Impl->Handle = callerFiber;
    }

    target->Impl->Opaque = opaque;
    SwitchToFiber(target->Impl->Handle);
    return caller->Impl->Opaque;
}

#else
#   error Unsupported platform
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NTY

