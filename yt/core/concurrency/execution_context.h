#pragma once

#include <util/system/defaults.h>

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

class TExecutionStack;

#if defined(_unix_)
typedef void (*TTrampoline)(void*);
struct TExecutionContext
{
    void* SP;
#   ifdef CXXABIv1
    __cxxabiv1::__cxa_eh_globals EH;
#   endif
};
#elif defined(_win_)
#define YT_TRAMPOLINE void
typedef void (*TTrampoline)(void*);
struct TExecutionContext
{
    TExecutionContext(const TExecutionContext&) = delete;
    TExecutionContext(TExecutionContext&&);
    ~TExecutionContext();

    void* Handle_;
    void* Opaque_;
    void (*Callee_)(void*);
};
#else
#   error Unsupported platform
#endif

TExecutionContext CreateExecutionContext(
    TExecutionStack& stack,
    TTrampoline trampoline);

void* SwitchExecutionContext(
    TExecutionContext* caller,
    TExecutionContext* target,
    void* opaque);

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NTY

