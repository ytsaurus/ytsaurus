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

TExecutionContext CreateExecutionContext(TExecutionStack& stack, TTrampoline trampoline)
{
    TExecutionContext result;
    memset(&result, 0, sizeof(result));

    auto sp = reinterpret_cast<void**>(reinterpret_cast<char*>(stack.GetStack()) + stack.GetSize());
    // We pad an extra nullptr to align %rsp before callq after jmpq.
    // Effectively, this nullptr mimics a return address.
    *--sp = nullptr;
    *--sp = reinterpret_cast<void*>(trampoline);
    // No need to set any extra registers, so just pad for them.
    sp -= 6;
    result.SP = sp;

    return result;
}

void* SwitchExecutionContext(TExecutionContext* caller, TExecutionContext* target, void* opaque)
{
#ifdef CXXABIv1
    auto* eh = __cxxabiv1::__cxa_get_globals_fast();
    caller->EH = *eh;
    *eh = target->EH;
#endif
    return SwitchExecutionContextImpl(&caller->SP, &target->SP, opaque);
}

#elif defined(_win_)

TExecutionContext::TExecutionContext(TExecutionContext&& other)
    : Handle_(other.Handle_)
    , Opaque_(other.Opaque_)
    , Callee_(other.Callee_)
{
    other.Handle_ = nullptr;
    other.Opaque_ = nullptr;
    other.Callee_ = nullptr;
}

TExecutionContext::~TExecutionContext()
{
    if (Handle_) {
        DeleteFiber(Handle_);
    }
}

VOID CALLBACK WinTrampoline(PVOID opaque)
{
    auto* context = reinterpret_cast<TExecutionContext*>(opaque);
    context->Callee_(context->Opaque_);
}

TExecutionContext CreateExecutionContext(const TExecutionStack& stack, TTrampoline callee)
{
    TExecutionContext result;
    result.Handle_ = CreateFiber(stack.GetSize(), &WinTrampoline, this);
    result.Opaque_ = nullptr;
    result.Callee_ = callee;
    return result;
}

void* SwitchExecutionContext(TExecutionContext* caller, TExecutionContext* target, void* opaque)
{
    target->Opaque_ = opaque;
    if (!caller->Handle_) {
        caller->Handle_ = GetCurrentFiber();
        if (caller->Handle_ == 0 || target->Handle_ == (void*)0x1e00) {
            caller->Handle_ = ConvertThreadToFiber(0);
        }
    }
    SwitchToFiber(target->Handle_);
    return caller->Opaque_;
}

#else
#   error Unsupported platform
#endif


////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NTY

