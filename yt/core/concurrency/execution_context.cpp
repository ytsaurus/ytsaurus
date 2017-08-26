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
    sizeof(__cxxabiv1::__cxa_eh_globals) == TExecutionContext::EHSize,
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
    memset(EH_, 0, EHSize);
}

TExecutionContext::TExecutionContext(void* stackBottom, size_t stackSize)
    : SP_(nullptr)
#if defined(_asan_enabled_)
    , San_(stackBottom, stackSize)
#endif
{
#if !defined(_asan_enabled_)
    Y_UNUSED(stackBottom);
    Y_UNUSED(stackSize);
#endif
    memset(EH_, 0, EHSize);
}

TExecutionContext::TExecutionContext(TExecutionContext&& other)
{
    SP_ = other.SP_;
    other.SP_ = nullptr;

#if defined(_asan_enabled_)
    San_ = std::move(other.San_);
#endif

#ifdef CXXABIv1
    memcpy(EH_, other.EH_, EHSize);
    memset(other.EH_, 0, EHSize);
#endif
}

#if defined(_asan_enabled_)
static void SanitizerTrampoline(
    void* opaque,
    void* rsi,
    void* rdx,
    void (*trampoline)(void*))
{
    NSan::TFiberContext::AfterStart();
    trampoline(opaque);
};
#endif

TExecutionContext CreateExecutionContext(
    TExecutionStack* stack,
    void (*trampoline)(void*))
{
    TExecutionContext context(stack->GetStack(), stack->GetSize());

    auto* sp = reinterpret_cast<void**>(reinterpret_cast<char*>(stack->GetStack()) + stack->GetSize());
    // We pad an extra nullptr to align %rsp before callq after jmpq.
    // Effectively, this nullptr mimics a return address.
    *--sp = nullptr;
#if !defined(_asan_enabled_)
    *--sp = reinterpret_cast<void*>(trampoline);
    // No need to set any extra registers, so just pad for them.
    sp -= 6;
#else
    *--sp = reinterpret_cast<void*>(SanitizerTrampoline);
    sp -= 5;
    *--sp = reinterpret_cast<void*>(trampoline);
#endif
    context.SP_ = sp;

    return context;
}

void* SwitchExecutionContext(
    TExecutionContext* caller,
    TExecutionContext* target,
    void* opaque)
{
#if defined(_asan_enabled_)
    target->San_.BeforeSwitch();
    // XXX: Or BeforeFinish(). There is no "finish" flag in
    // SwitchExecutionContext interface now. It will leak when
    // using ASan with detect_stack_use_after_return option enabled.
#endif
#ifdef CXXABIv1
    auto* eh = __cxxabiv1::__cxa_get_globals();
    memcpy(caller->EH_, eh, TExecutionContext::EHSize);
    memcpy(eh, target->EH_, TExecutionContext::EHSize);
#endif
    void* result = SwitchExecutionContextImpl(&caller->SP_, &target->SP_, opaque);
#if defined(_asan_enabled_)
    target->San_.AfterSwitch();
#endif
    return result;
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

