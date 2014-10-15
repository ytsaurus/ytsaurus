#include "stack_trace.h"

#include <yt/config.h>

#ifndef _win_

#if defined(HAVE_UNWIND_H)
#   define ENABLE_GCC_STACKTRACE
#endif
#if defined(HAVE_EXECINFO_H)
#   define ENABLE_GLIBC_STACKTRACE
#endif
#if defined(HAVE_LIBUNWIND_H)
#   define ENABLE_LIBUNWIND_STACKTRACE
#endif
#if !defined(ENABLE_GCC_STACKTRACE) && !defined(ENABLE_GLIBC_STACKTRACE) && !defined(ENABLE_LIBUNWIND_STACKTRACE)
#   error "No feasible stack unwinding avaliable."
#endif

////////////////////////////////////////////////////////////////////////////////
// libgcc-based implementation.
////////////////////////////////////////////////////////////////////////////////
#ifdef ENABLE_GCC_STACKTRACE

extern "C" {
#include <stdlib.h>
#include <unwind.h>
}

namespace NYT {
namespace {

typedef struct {
    void **result;
    int maxFrames;
    int skipFrames;
    int frames;
} TStackTraceArgument;

// This code is not considered ready to run until static initializers run
// so that we are guaranteed that any malloc-related initialization is done.
static bool alreadyInitialized = false;

static _Unwind_Reason_Code NoopUnwinder(struct _Unwind_Context* uc, void* opaque)
{
    return _URC_NO_REASON;
}

class TStackTraceInitializer
{
public:
    TStackTraceInitializer()
    {
        // Extra call to force initialization.
        _Unwind_Backtrace(NoopUnwinder, NULL);
        alreadyInitialized = true;
    }
};

static TStackTraceInitializer initializer;

static _Unwind_Reason_Code StackTraceUnwinder(struct _Unwind_Context* uc, void *opaque)
{
    TStackTraceArgument* argument = (TStackTraceArgument*)opaque;

    if (argument->skipFrames > 0) {
        --argument->skipFrames;
    } else {
        argument->result[argument->frames] = (void*)_Unwind_GetIP(uc);
        ++argument->frames;
    }

    return argument->frames == argument->maxFrames ? _URC_END_OF_STACK : _URC_NO_REASON;
}

int GetStackTrace__libgcc(void** result, int maxFrames, int skipFrames)
{
    if (!alreadyInitialized) {
        return 0;
    }

    TStackTraceArgument argument;

    argument.result = result;
    argument.maxFrames = maxFrames;
    argument.skipFrames = skipFrames;
    argument.frames = 0;

    _Unwind_Backtrace(StackTraceUnwinder, &argument);

    return argument.frames;
}

} // namespace
} // namespace YT

#endif // ENABLE_GCC_STACKTRACE

////////////////////////////////////////////////////////////////////////////////
// glibc-based implementation.
////////////////////////////////////////////////////////////////////////////////
#ifdef ENABLE_GLIBC_STACKTRACE

extern "C" {
#include <execinfo.h>
}

namespace NYT {
namespace {

// Note: The glibc implementation may cause a call to malloc.
int GetStackTrace__glibc(void** result, int maxFrames, int skipFrames)
{
    static const int MaximalStackDepth = 128;
    void* localFrames[MaximalStackDepth];

    int frames;
    frames = backtrace(localFrames, MaximalStackDepth);
    frames = frames - skipFrames;
    frames = frames > 0 ? frames : 0;
    frames = frames < maxFrames ? frames : maxFrames;

    for (int i = 0; i < frames; ++i) {
        result[i] = localFrames[i + skipFrames];
    }

    return frames;
}

} // namespace
} // namespace YT

#endif // ENABLE_GLIBC_STACKTRACE

////////////////////////////////////////////////////////////////////////////////
// libunwind-based implementation.
////////////////////////////////////////////////////////////////////////////////
#ifdef ENABLE_LIBUNWIND_STACKTRACE

extern "C" {
#define UNW_LOCAL_ONLY
#include <libunwind.h>
}

namespace NYT {
namespace {

// Sometimes, we can try to get a stack trace from within a stack trace,
// because libunwind can call mmap (maybe indirectly via an internal mmap
// based memory allocator), and that mmap gets trapped and causes a stack trace
// request. If were to try to honor that recursive request, we'd end up
// with infinite recursion or deadlock. Luckily, it's safe to ignore those
// subsequent traces. In such cases, we return 0 to indicate the situation.

static bool alreadyUnwinding = false;

int GetStackTrace__libunwind(void** result, int maxFrames, int skipFrames)
{
    void* ip;
    unw_cursor_t cursor;
    unw_context_t context;

    int frames = 0;

    if (__sync_val_compare_and_swap(&alreadyUnwinding, false, true)) {
        return 0;
    }

    if (unw_getcontext(&context) != 0) {
        abort();
    }
    if (unw_init_local(&cursor, &context) != 0) {
        abort();
    }

    while (frames < maxFrames) {
        int rv = unw_get_reg(&cursor, UNW_REG_IP, (unw_word_t *)&ip);
        if (rv < 0) {
            break;
        }

        if (skipFrames > 0) {
            --skipFrames;
        } else {
            result[frames] = ip;
            ++frames;
        }

        rv = unw_step(&cursor);
        if (rv <= 0) {
            break;
        }
    }

    alreadyUnwinding = false;
    return frames;
}

} // namespace
} // namespace YT

#endif // ENABLE_LIBUNWIND_STACKTRACE

////////////////////////////////////////////////////////////////////////////////
// At last, the entry point.
////////////////////////////////////////////////////////////////////////////////

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

int GetStackTrace(void** result, int maxFrames, int skipFrames)
{
    // Ignore this frame and subsequent frame (which is |GetStackTrace__impl|).
    skipFrames += 2;
#if 1 < 0
    // :)
#elif defined(ENABLE_LIBUNWIND_STACKTRACE)
    return GetStackTrace__libunwind(result, maxFrames, skipFrames);
#elif defined(ENABLE_GCC_STACKTRACE)
    return GetStackTrace__libgcc(result, maxFrames, skipFrames);
#elif defined(ENABLE_GLIBC_STACKTRACE)
    return GetStackTrace__glibc(result, maxFrames, skipFrames);
#else
    return 0;
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#endif
