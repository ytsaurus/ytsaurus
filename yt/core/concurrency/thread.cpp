#include "stdafx.h"
#include "thread.h"

#include <util/system/thread.i>

#ifdef _unix_
#include <pthread.h>
#include <dlfcn.h>
#ifndef _darwin_
#include <sys/prctl.h>
#endif
#endif

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

void SetCurrentThreadName(const char* name)
{
#if defined(_win_)

    // http://msdn.microsoft.com/en-us/library/xcb2z8hs%28VS.80%29.aspx

    typedef struct tagTHREADNAME_INFO
    {
        DWORD dwType;     // Must be 0x1000.
        LPCSTR szName;    // Pointer to name (in user address space).
        DWORD dwThreadID; // Thread ID (-1 is the caller thread).
        DWORD dwFlags;    // Reserved for future use, must be zero.
    } THREADNAME_INFO;

    THREADNAME_INFO info;
    info.dwType = 0x1000;
    info.szName = static_cast<LPCSTR>(name);
    info.dwThreadID = static_cast<DWORD>(-1);
    info.dwFlags = 0;

    __try
    {
#define MS_VC_EXCEPTION 0x406D1388
        RaiseException(MS_VC_EXCEPTION, 0, sizeof(info) / sizeof(DWORD), (ULONG_PTR *)&info);
#undef MS_VC_EXCEPTION
    }
    __except(EXCEPTION_CONTINUE_EXECUTION)
    {
    }

#elif defined(_unix_)

#if defined(_darwin_)
// Mac OS X does not expose the length limit of the name.
#define _MAX_THREAD_NAME_LENGTH_ 64
#else
// This limit comes from glibc, which gets it from the kernel (TASK_COMM_LEN).
#define _MAX_THREAD_NAME_LENGTH_ 16
#endif

    // http://0pointer.de/blog/projects/name-your-threads.html
    // http://src.chromium.org/viewvc/chrome/trunk/src/base/threading/platform_thread_posix.cc?view=markup

    // (Unix)
    // glibc recently added support for pthread_setname_np, but it's not
    // commonly available yet.
    // (Mac OS X)
    // pthread_setname_np is only available in 10.6 or later.

    int (*dynamic_pthread_setname_np)(pthread_t, const char*);
    *reinterpret_cast<void**>(&dynamic_pthread_setname_np) =
        dlsym(RTLD_DEFAULT, "pthread_setname_np");

    if (dynamic_pthread_setname_np) {
        if (::strlen(name) >= _MAX_THREAD_NAME_LENGTH_) {
            fprintf(stderr, "Thread name '%s' will be truncated\n", name);
        }

        char truncatedName[_MAX_THREAD_NAME_LENGTH_];
        ::memset(truncatedName, 0, _MAX_THREAD_NAME_LENGTH_);
        ::strncpy(truncatedName, name, _MAX_THREAD_NAME_LENGTH_ - 1);

        int error = dynamic_pthread_setname_np(pthread_self(), truncatedName);
        if (error < 0) {
            // TODO(sandello): Add safe enough strerror() call here.
            fprintf(stderr, "Failed to set thread name (Call: prctl(), ErrNo: %d)\n",
                error);
            abort();
        }
    } else {
#if defined(_darwin_)
        fprintf(stderr, "Failed to set thread name (Unsupported)\n");
        abort();
#else
        // Implementing this function without glibc is simple enough.
        // (We don't do the name length clipping as above because it will be
        // truncated by the callee (see TASK_COMM_LEN above).)
        int error = prctl(PR_SET_NAME, name);
        if (error < 0) {
            // TODO(sandello): Add safe enough strerror() call here.
            fprintf(stderr, "Failed to set thread name (Call: prctl(), ErrNo: %d)\n",
                error);
            abort();
        }
#endif
    }

#undef _MAX_THREAD_NAME_LENGTH_
#endif
}

TThreadId GetCurrentThreadId()
{
    // TODO(babenko): add support for other platforms using some well-established TLS macros
#if defined(__GNUC__) && !defined(__APPLE__)
    static PER_THREAD auto CachedThreadId = InvalidThreadId;
    if (CachedThreadId == InvalidThreadId) {
        CachedThreadId = SystemCurrentThreadIdImpl();
    }
    return CachedThreadId;
#else
    return SystemCurrentThreadIdImpl();
#endif
}

bool RaiseCurrentThreadPriority()
{
#ifdef _win_
    return SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_HIGHEST) != 0;
#else
    struct sched_param param = { };
    param.sched_priority = 31;
    return pthread_setschedparam(pthread_self(), SCHED_RR, &param) == 0;
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
