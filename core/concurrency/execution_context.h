#pragma once

#include <util/system/defaults.h>
#include <util/system/sanitizers.h>

#if defined(__GNUC__) || defined(__clang__)
#   define CXXABIv1
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
    TExecutionContext(void* stackBottom, size_t stackSize);
    TExecutionContext(TExecutionContext&& other);
    TExecutionContext(const TExecutionContext&) = delete;

#ifdef CXXABIv1
    static const int EHSize = 16;
#endif

private:
    void* SP_;

#ifdef CXXABIv1
    char EH_[EHSize];
#endif

#if defined(_asan_enabled_)
    NSan::TFiberContext San_;
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

