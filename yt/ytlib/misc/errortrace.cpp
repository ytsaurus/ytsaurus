// For some references, see:
// http://www.tlug.org.za/wiki/index.php/Obtaining_a_stack_trace_in_C_upon_SIGSEGV
// http://google.com/codesearch/p?hl=en&sa=N&cd=1&ct=rc#HT3Jwvgod1I/glibc-2.5/debug/backtrace.c&q=file:%22backtrace.c%22%20lang:c%20package:glibc
// http://google.com/codesearch/p?hl=en#BdqTRioUGj8/backtrace.c&q=bt_%20backtrace%20lang:c%20file:backtrace.c
//

#include "stdafx.h"
#include <typeinfo>
#include <exception>
#include <util/system/platform.h>
#include "errortrace.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static TErrorContext* ErrContext = 0;

TErrorContext::TErrorContext()
{
    ErrContext = this;
}
TErrorContext::~TErrorContext()
{
    ErrContext = 0;
}

////////////////////////////////////////////////////////////////////////////////


#if defined(_win_)

int SetupErrorHandler()
{
    return 1;
}

#else //_win_

#include <memory.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <dlfcn.h>
#include <cxxabi.h>
#include <sys/time.h>
#include <time.h>

#if defined(_darwin_)
#include <sys/ucontext.h>
#else
#include <ucontext.h>
#endif

////////////////////////////////////////////////////////////////////////////////

inline const char* LocalTimeToStr(char *buf, size_t maxsize)
{
    *buf = '\0';
    struct timeval tv;
    gettimeofday(&tv, 0);
    time_t sec = (time_t)tv.tv_sec;
    struct tm time;
    localtime_r(&sec, &time);
    strftime(buf, maxsize, "%Y-%m-%dT%H:%M:%S", &time);
    return buf;
}

////////////////////////////////////////////////////////////////////////////////

// gcc's backtrace() (so as backtrace_symbols()) is available on freebsd
// only as separate library from ports (libexecinfo).
// But we does not have it installed on our freebsd machines.
//
// util/system/backtrace.c:BackTrace() with it's fallback used if backtrace()
// is absent just doesn't work on amd64 freebsd.
// It cannot be used either.
//
// backtrace based on __builtin_{return,frame}_address
// shown to be unstable, could segfault sometimes.
// Cannot be used either.
//
// So we have there local analog of backtrace() and backtrace_symbols_fd()
// merged into single function (reason: stack traversing is prone to errors
// so it better to print every step immediately).

static const char * print_symbol(FILE *f, int frame_index, void *const addr)
{
    Dl_info info;
    if (dladdr(addr, &info) != 0) {
        if (info.dli_saddr == NULL)
            info.dli_saddr = addr;

        const char *symname = info.dli_sname;
        int status;
        char *tmp = __cxxabiv1::__cxa_demangle(symname, NULL, 0, &status);

        if (status == 0 && tmp)
            symname = tmp;
        fprintf(f, "% 2d: %p <%s+%d> (%s)\n",
                frame_index,
                addr,
                symname,
                (unsigned int)((char*)addr - (char*)info.dli_saddr),
                info.dli_fname);

        if (tmp)
            free(tmp);

        return symname;

    } else {
        fprintf(f, "% 2d: %p\n", frame_index, addr);

        return 0;
    }
}

// analog of backtrace() but traces from specified frame address
int glibc_backtrace(void ** bt, int size, void ** frame, void * addr)
{
    int i = 0;
    while (frame && addr && i < size) {
        if (frame < (void**)0x100 || addr < (void*)0x100) {
            // special guard as we don't really know stack limits
            // and stack can go wild
            break;
        }
        bt[i++] = addr;
        addr = frame[1];
        frame = (void**)frame[0];
    }
    return i;
}

void print_backtrace_symbols(FILE *f, void *const *bt, int size)
{
    for (int i = 0; i < size; ++i) {
        // frame numbers are 1-based
        const char * name = print_symbol(f, i + 1, bt[i]);
        if (name && strcmp(name, "main") == 0) {
            break;
        }
    }
}

// Collects and prints backtrace simultaneously, one frame at a time,
// to give as many information as possible in case if some memory error
// occurs (which are quite common).
static int print_backtrace(FILE *f, int depth, void ** frame, void * addr)
{
    int i = 0;
    while (frame && addr && i < depth) {
        if (frame < (void**)0x100 || addr < (void*)0x100) {
            // special guard as we don't really know stack limits
            // and stack can go wild
            break;
        }
        // frame numbers are 1-based
        const char * name = print_symbol(f, ++i, addr);
        if (name && strcmp(name, "main") == 0) {
            break;
        }
        addr = frame[1];
        frame = (void**)frame[0];
    }
    return i;
}

////////////////////////////////////////////////////////////////////////////////

// Common handler for all "bad code" errors: SIGILL, SIGFPE, SIGSEGV, SIGBUS
static void signal_handler(int, siginfo_t * info, void * context)
{
    char datebuf[20];
    fprintf(stderr, "%s", LocalTimeToStr(datebuf, sizeof(datebuf)));
    fprintf(stderr, " Signal %d (%s): address=%p, code=%d, errno=%d\n",
            info->si_signo,
            strsignal(info->si_signo),
            info->si_addr,
            info->si_code,
            info->si_errno
            );

    // if there is other error in progress, don't bother with additional traces
    static volatile sig_atomic_t error_in_progress = 0;
    if (error_in_progress == 0) {
        error_in_progress = 1;

        // User error context printed first, in case of failure in backtracing code
        if (ErrContext != 0) {
            const int SIZE = 1000;
            char buffer[SIZE] = "\0";
            ErrContext->ToString(buffer, SIZE);
            fputs(buffer, stderr);
            fprintf(stderr, "\n");
            ErrContext = 0;
        }

        void *ip = 0;
        void **bp = 0;
        {
            ucontext_t *ucontext = (ucontext_t*)context;

#if defined(_linux_)

#if defined(_i386_) // ia32
            ip = (void*)ucontext->uc_mcontext.gregs[REG_EIP];
            bp = (void**)ucontext->uc_mcontext.gregs[REG_EBP];
#elif defined(_x86_64_) // ia64
            ip = (void*)ucontext->uc_mcontext.gregs[REG_RIP];
            bp = (void**)ucontext->uc_mcontext.gregs[REG_RBP];
#else
            #error "Unsupported Linux host architecture"
#endif

#elif defined(_freebsd_)

#if defined(_i386_) // ia32
            ip = (void*)ucontext->uc_mcontext.mc_eip;
            bp = (void**)ucontext->uc_mcontext.mc_ebp;
#elif defined(_x86_64_) // ia64
            ip = (void*)ucontext->uc_mcontext.mc_rip;
            bp = (void**)ucontext->uc_mcontext.mc_rbp;
#else
            #error "Unsupported FreeBSD host architecture"
#endif

#elif defined(_darwin_)

#if defined(_i386_) // ia32
#if __DARWIN_UNIX03
            ip = (void*)ucontext->uc_mcontext->__ss.__eip;
            bp = (void**)ucontext->uc_mcontext->__ss.__ebp;
#else  // !__DARWIN_UNIX03
            ip = (void*)ucontext->uc_mcontext->ss.eip;
            bp = (void**)ucontext->uc_mcontext->ss.ebp;
#endif  // __DARWIN_UNIX03
#elif defined(_x86_64_) // ia64
#if __DARWIN_UNIX03
            ip = (void*)ucontext->uc_mcontext->__ss.__rip;
            bp = (void**)ucontext->uc_mcontext->__ss.__rbp;
#else  // !__DARWIN_UNIX03
            ip = (void*)ucontext->uc_mcontext->ss.rip;
            bp = (void**)ucontext->uc_mcontext->ss.rbp;
#endif  // __DARWIN_UNIX03
#else
            #error "Unsupported Mac OS X host architecture"
#endif

#else
            #error "Unsupported target OS"
#endif
        }

        const int DEPTH = 20;

        fprintf(stderr, "Stack trace (from frame=%p, addr=%p):\n", bp, ip);
        
        print_backtrace(stderr, DEPTH, bp, ip);
        
        // Equivalent by less robust backtracing using glibc_backtrace:
        // void* bt[DEPTH];
        // glibc_backtrace(bt, DEPTH, bp, ip);
        // print_backtrace_symbols(stderr, bt, DEPTH);

        fprintf(stderr, "End of stack trace\n");
    }

    // reraise signal, now with default action,
    // to not disturb expected behaviour (exit effects including exit status)
    signal(info->si_signo, SIG_DFL);
    raise(info->si_signo);
}

int SetupErrorHandler()
{
    struct sigaction a;
    memset(&a, 0, sizeof(a));
    a.sa_sigaction = signal_handler;
    a.sa_flags = SA_SIGINFO;

    int sig[] = {SIGILL, SIGFPE, SIGSEGV, SIGBUS, SIGABRT};
    for (size_t i = 0; i < sizeof(sig)/sizeof(sig[0]); ++i) {
        if (sigaction(sig[i], &a, 0) < 0) {
            perror("sigaction");
            return 1;
        }
    }
    return 0;
}

#endif //_unix_

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
