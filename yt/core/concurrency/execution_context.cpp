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

#ifdef CXXABIv1

#ifdef YT_IN_ARCADIA

#include <cxxabi.h>

#else

// MSVC compiler has /GT option for supporting fiber-safe thread-local storage.
// For CXXABIv1-compliant systems we can hijack __cxa_eh_globals.
// See http://mentorembedded.github.io/cxx-abi/abi-eh.html
namespace __cxxabiv1 {
// We do not care about actual type here, so erase it.
typedef void __untyped_cxa_exception;
struct __cxa_eh_globals
{
    __untyped_cxa_exception* caughtExceptions;
    unsigned int uncaughtExceptions;
};
extern "C" __cxa_eh_globals* __cxa_get_globals();
extern "C" __cxa_eh_globals* __cxa_get_globals_fast();
} // namespace __cxxabiv1

#endif // YT_IN_ARCADIA

#endif // CXXABIv1

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

#ifdef CXXABIv1
static_assert(
    sizeof(__cxxabiv1::__cxa_eh_globals) == TExecutionContext::EH_SIZE,
    "Size mismatch of __cxa_eh_globals structure");
#endif

#if defined(_unix_)

extern "C" void* __attribute__((__regparm__(3))) SwitchExecutionContextImpl(
    void** caller,
    void** target,
    void* opaque);

TExecutionContext::TExecutionContext()
    : SP_(nullptr)
{
    memset(EH_, 0, EH_SIZE);
}

TExecutionContext::TExecutionContext(TExecutionContext&& other)
{
    SP_ = other.SP_;
    other.SP_ = nullptr;

#ifdef CXXABIv1
    memcpy(EH_, other.EH_, EH_SIZE);
    memset(other.EH_, 0, EH_SIZE);
#endif
}

TExecutionContext CreateExecutionContext(
    TExecutionStack* stack,
    void (*trampoline)(void*))
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
    context.SP_ = sp;

    return context;
}

void* SwitchExecutionContext(
    TExecutionContext* caller,
    TExecutionContext* target,
    void* opaque)
{
#ifdef CXXABIv1
    auto* eh = __cxxabiv1::__cxa_get_globals();
    memcpy(caller->EH_, eh, TExecutionContext::EH_SIZE);
    memcpy(eh, target->EH_, TExecutionContext::EH_SIZE);
#endif
    return SwitchExecutionContextImpl(&caller->SP_, &target->SP_, opaque);
}

#elif defined(_win_)

TExecutionContext::TExecutionContext()
    : Handle_(nullptr)
{ }

TExecutionContext::TExecutionContext(TExecutionContext&& other)
{
    Handle_ = other.Handle_;
    other.Handle_ = nullptr;
}

TExecutionContext CreateExecutionContext(
    TExecutionStack* stack,
    void (*trampoline)(void*))
{
    stack->SetTrampoline(trampoline);

    TExecutionContext context;
    context.Handle_ = stack->Handle_;
    return context;
}

void* SwitchExecutionContext(
    TExecutionContext* caller,
    TExecutionContext* target,
    void* opaque)
{
    auto callerHandle = GetCurrentFiber();
    if (callerHandle == (void*)0x0 || callerHandle == (void*)0x1e00) {
        callerHandle = ConvertThreadToFiber(0);
    }
    caller->Handle_ = callerHandle;

    TExecutionStack::SetOpaque(opaque);
    SwitchToFiber(target->Handle_);
    return TExecutionStack::GetOpaque();
}

#else
#   error Unsupported platform
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NTY

