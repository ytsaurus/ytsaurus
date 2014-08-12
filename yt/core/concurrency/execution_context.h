#pragma once

#include <util/system/defaults.h>

#include <config.h>

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
struct __cxa_eh_globals
{
    __untyped_cxa_exception* caughtExceptions;
    unsigned int uncaughtExceptions;
};

// TODO(babenko): investigate
#ifdef _darwin_
extern "C" __cxa_eh_globals* __cxa_get_globals();
extern "C" __cxa_eh_globals* __cxa_get_globals_fast();
#endif

} // namespace __cxxabiv1
#endif

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TExecutionStack;

#if defined(_unix_)

class TExecutionContext
{
public:
    TExecutionContext();
    TExecutionContext(TExecutionContext&& other);
    TExecutionContext(const TExecutionContext&) = delete;

private:
    void* SP_;
#ifdef CXXABIv1
    __cxxabiv1::__cxa_eh_globals EH_;
#endif

    friend TExecutionContext CreateExecutionContext(
        TExecutionStack* stack,
        void (*trampoline)(void*));
    friend void* SwitchExecutionContext(
        TExecutionContext* caller,
        TExecutionContext* target,
        void* opaque);

};

#elif defined(_win_)

class TExecutionContext
{
public:
    TExecutionContext();
    TExecutionContext(TExecutionContext&& other);
    TExecutionContext(const TExecutionContext&) = delete;

private:
    void* Handle_;

    friend TExecutionContext CreateExecutionContext(
        TExecutionStack* stack,
        void (*trampoline)(void*));
    friend void* SwitchExecutionContext(
        TExecutionContext* caller,
        TExecutionContext* target,
        void* opaque);

};

#else
#   error Unsupported platform
#endif

TExecutionContext CreateExecutionContext(
    TExecutionStack* stack,
    void (*trampoline)(void*));

void* SwitchExecutionContext(
    TExecutionContext* caller,
    TExecutionContext* target,
    void* opaque);

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NTY

