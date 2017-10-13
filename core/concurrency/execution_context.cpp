#include "execution_context.h"
#include "execution_stack.h"

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

} // namespace __cxxabiv1

#endif // YT_IN_ARCADIA

#endif // CXXABIv1

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TExecutionContext::TExecutionContext(
    TExecutionStack* stack,
    ITrampoLine* trampoline)
    : ContClosure_{
        trampoline,
        TMemRegion(static_cast<char*>(stack->GetStack()), stack->GetSize())}
    , ContContext_(ContClosure_)
{
#ifdef CXXABIv1
    static_assert(
        sizeof(__cxxabiv1::__cxa_eh_globals) == TExecutionContext::EHSize,
        "Size mismatch of __cxa_eh_globals structure");
#endif
}

void TExecutionContext::SwitchTo(TExecutionContext* target)
{
#ifdef CXXABIv1
    auto* eh = __cxxabiv1::__cxa_get_globals();
    ::memcpy(EH_.data(), eh, EHSize);
    ::memcpy(eh, target->EH_.data(), EHSize);
#endif
    ContContext_.SwitchTo(&target->ContContext_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NTY

