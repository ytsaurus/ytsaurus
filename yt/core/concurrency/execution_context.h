#pragma once

#include <util/system/defaults.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

// MSVC compiler has /GT option for supporting fiber-safe thread-local storage.
// For CXXABIv1-compliant systems we can hijack __cxa_eh_globals.
// See http://mentorembedded.github.io/cxx-abi/abi-eh.html
#if defined(__GNUC__) || defined(__clang__)
#   define CXXABIv1
#   ifdef HAVE_CXXABI_H
#       include <cxxabi.h>
#   endif
#   ifdef _GLIBCXX_NOTHROW
#       define CXXABIv1_NOTHROW _GLIBCXX_NOTHROW
#   else
#       define CXXABIv1_NOTHROW throw()
#   endif
namespace __cxxabiv1 {
    // We do not care about actual type here, so erase it.
    typedef void __untyped_cxa_exception;
    struct __cxa_eh_globals {
        __untyped_cxa_exception* caughtExceptions;
        unsigned int uncaughtExceptions;
    };
    extern "C" __cxa_eh_globals* __cxa_get_globals() CXXABIv1_NOTHROW;
    extern "C" __cxa_eh_globals* __cxa_get_globals_fast() CXXABIv1_NOTHROW;
} // namespace __cxxabiv1
#endif

////////////////////////////////////////////////////////////////////////////////

class TExecutionStack;
typedef void (*TTrampoline)(void*);

#if defined(_unix_)

struct TExecutionContext
{
    TExecutionContext();
    TExecutionContext(TExecutionContext&& other);
    TExecutionContext(const TExecutionContext&) = delete;

    void* SP;
#ifdef CXXABIv1
    __cxxabiv1::__cxa_eh_globals EH;
#endif
};

#elif defined(_win_)

struct TExecutionContextImpl
{
    TExecutionContextImpl();
    TExecutionContextImpl(TExecutionContextImpl&& other);
    TExecutionContextImpl(const TExecutionContextImpl&) = delete;
    ~TExecutionContextImpl();

    void* Handle;
    bool Owning;
    void* Opaque;
    void (*Callee)(void*);
};

struct TExecutionContext
{
    TExecutionContext();

    std::unique_ptr<TExecutionContextImpl> Impl;
};

#else
#   error Unsupported platform
#endif

TExecutionContext CreateExecutionContext(
    TExecutionStack* stack,
    TTrampoline trampoline);

void* SwitchExecutionContext(
    TExecutionContext* caller,
    TExecutionContext* target,
    void* opaque);

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NTY

