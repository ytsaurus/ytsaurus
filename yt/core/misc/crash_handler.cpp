#include "crash_handler.h"
#include "stack_trace.h"
#include "assert.h"

#include <yt/config.h>

#include <core/misc/raw_formatter.h>

#include <core/logging/log_manager.h>

#include <util/system/defaults.h>

// This fixes inclusion error under GCC 4.5.
#include <exception>

#include <signal.h>
#include <time.h>
#ifdef HAVE_SYS_TYPES_H
#   include <sys/types.h>
#endif
#ifdef HAVE_UNISTD_H
#   include <unistd.h>
#endif
#ifdef HAVE_UCONTEXT_H
#   include <ucontext.h>
#endif
#ifdef HAVE_SYS_UCONTEXT_H
#   include <sys/ucontext.h>
#endif
#ifdef HAVE_DLFCN_H
#   include <dlfcn.h>
#endif
#ifdef HAVE_CXXABI_H
#   include <cxxabi.h>
#endif
#ifdef HAVE_PTHREAD_H
#   include <pthread.h>
#endif

#include <cstdlib>
#include <cstring>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

#ifdef _unix_
namespace {

////////////////////////////////////////////////////////////////////////////////

// See http://pubs.opengroup.org/onlinepubs/009695399/functions/xsh_chap02_04.html
// for a list of async signal safe functions.

// We will install the failure signal handler for these signals.
// We could use strsignal() to get signal names, but we do not use it to avoid
// introducing yet another #ifdef complication.
const struct {
    int Number;
    const char *Name;
} FailureSignals[] = {
    { SIGSEGV, "SIGSEGV" },
    { SIGILL,  "SIGILL"  },
    { SIGFPE,  "SIGFPE"  },
    { SIGABRT, "SIGABRT" },
    { SIGBUS,  "SIGBUS"  },
    { SIGTERM, "SIGTERM" },
};

//! Returns the program counter from a signal context, NULL if unknown.
void* GetPC(void* uc)
{
    // TODO(sandello): Merge with code from Bind() internals.
#if (defined(HAVE_UCONTEXT_H) || defined(HAVE_SYS_UCONTEXT_H)) && defined(PC_FROM_UCONTEXT)
    if (uc != NULL) {
        ucontext_t* context = reinterpret_cast<ucontext_t*>(uc);
        return (void*)context->PC_FROM_UCONTEXT;
    }
#endif
    return NULL;
}

//! Returns the symbol for address at given program counter.
int GetSymbol(void* pc, char* buffer, int length)
{
    TRawFormatter<0> formatter(buffer, length);

#if defined(HAVE_DLFCN_H)
    // See http://www.codesourcery.com/cxx-abi/abi.html#mangling
    // And, yes, dladdr() is not async signal safe. We can substitute it
    // with hand-written symbolization code from google-glog in case of any trouble.
    Dl_info info;
    if (!dladdr(pc, &info)) {
        return 0;
    }

    /*
     * typedef struct {
     *     const char *dli_fname;  // Pathname of shared object that
     *                             // contains address
     *     void       *dli_fbase;  // Address at which shared object
     *                             // is loaded
     *     const char *dli_sname;  // Name of nearest symbol with address
     *                             // lower than addr
     *     void       *dli_saddr;  // Exact address of symbol named
     *                             // in dli_sname
     * } Dl_info;
     *
     * If no symbol matching addr could be found, then dli_sname and dli_saddr are set to NULL.
     */

    if (info.dli_sname && info.dli_saddr) {
        formatter.AppendString("<");
#if defined(HAVE_CXXABI_H)
        int demangleStatus = 0;

        if (info.dli_sname[0] == '_' && info.dli_sname[1] == 'Z') {
            // This is also not async signal safe.
            // But (ta-dah!) we can replace it with symbolization code from google-glob.
            char* demangledName = abi::__cxa_demangle(info.dli_sname, 0, 0, &demangleStatus);
            if (demangleStatus == 0) {
                formatter.AppendString(demangledName);
            } else {
                formatter.AppendString(info.dli_sname);
            }
            free(demangledName);
        } else {
            formatter.AppendString(info.dli_sname);
        }
#else
        formatter.AppendString(info.dli_sname);
#endif
        formatter.AppendString("+");
        formatter.AppendNumber((char*)pc - (char*)info.dli_saddr);
        formatter.AppendString(">");
        formatter.AppendString(" ");
    }

    if (info.dli_fname && info.dli_fbase) {
        formatter.AppendString("(");
        formatter.AppendString(info.dli_fname);
        formatter.AppendString("+");
        formatter.AppendNumber((char*)pc - (char*)info.dli_fbase);
        formatter.AppendString(")");
    }
#else
    formatter.AppendString("0x");
    formatter.AppendNumber((uintptr_t)pc, 16);
#endif
    return formatter.GetBytesWritten();
}

//! Writes the given buffer with the length to the standard error.
void WriteToStderr(const char* buffer, int length)
{
    if (write(2, buffer, length) < 0) {
        // Ignore errors.
    }
}

//! Dumps time information.
/*!
 *  We do not dump human-readable time information with localtime()
 *  as it is not guaranteed to be async signal safe.
 */
void DumpTimeInfo()
{
    time_t timeSinceEpoch = time(NULL);

    TRawFormatter<256> formatter;

    formatter.AppendString("*** Aborted at ");
    formatter.AppendNumber(timeSinceEpoch);
    formatter.AppendString(" (Unix time); Try \"date -d @");
    formatter.AppendNumber(timeSinceEpoch, 10);
    formatter.AppendString("\" if you are using GNU date ***\n");

    WriteToStderr(formatter.GetData(), formatter.GetBytesWritten());
}

//! Dumps information about the signal.
void DumpSignalInfo(int signal, siginfo_t* si)
{
    // Get the signal name.
    const char* name = NULL;
    for (size_t i = 0; i < ARRAY_SIZE(FailureSignals); ++i) {
        if (signal == FailureSignals[i].Number) {
            name = FailureSignals[i].Name;
        }
    }

    TRawFormatter<256> formatter;

    formatter.AppendString("*** ");
    if (name) {
        formatter.AppendString(name);
    } else {
        // Use the signal number if the name is unknown. The signal name
        // should be known, but just in case.
        formatter.AppendString("Signal ");
        formatter.AppendNumber(si->si_signo);
    }

    formatter.AppendString(" (@0x");
    formatter.AppendNumber(reinterpret_cast<uintptr_t>(si->si_addr), 16);
    formatter.AppendString(")");
    formatter.AppendString(" received by PID ");
    formatter.AppendNumber(getpid());
    formatter.AppendString(" (TID 0x");
    // We assume pthread_t is an integral number or a pointer, rather
    // than a complex struct. In some environments, pthread_self()
    // returns an uint64 but in some other environments pthread_self()
    // returns a pointer. Hence we use C-style cast here, rather than
    // reinterpret/static_cast, to support both types of environments.
    formatter.AppendNumber((uintptr_t)pthread_self(), 16);
    formatter.AppendString(") ");
    // Only linux has the PID of the signal sender in si_pid.
#ifdef _linux_
    formatter.AppendString("from PID ");
    formatter.AppendNumber(si->si_pid);
    formatter.AppendString(" ");
#endif
    formatter.AppendString("***\n");

    WriteToStderr(formatter.GetData(), formatter.GetBytesWritten());
}

//! Dumps information about the stack frame.
void DumpStackFrameInfo(void* pc)
{
    TRawFormatter<1024> formatter;

    formatter.AppendString("@ ");
    const int width = (sizeof(void*) == 8 ? 12 : 8) + 2;
    // +2 for "0x"; 12 for x86_64 because higher bits are always zeroed.
    formatter.AppendNumberAsHexWithPadding(reinterpret_cast<uintptr_t>(pc), width);
    formatter.AppendString(" ");
    // Get the symbol from the previous address of PC,
    // because PC may be in the next function.
    formatter.Advance(GetSymbol(
        reinterpret_cast<char*>(pc) - 1,
        formatter.GetCursor(),
        formatter.GetBytesRemaining()));
    formatter.AppendString("\n");

    WriteToStderr(formatter.GetData(), formatter.GetBytesWritten());
}

//! Invoke the default signal handler.
void InvokeDefaultSignalHandler(int signal)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = SIG_DFL;
    YCHECK(sigaction(signal, &sa, NULL) == 0);

    kill(getpid(), signal);
}

// This variable is used for protecting CrashSignalHandler() from
// dumping stuff while another thread is doing it. Our policy is to let
// the first thread dump stuff and let other threads wait.
static pthread_t* crashingThreadId = NULL;

// Dumps signal and stack frame information, and invokes the default
// signal handler once our job is done.
void CrashSignalHandler(int signal, siginfo_t* si, void* uc)
{
    // All code here _MUST_ be async signal safe unless specified otherwise.

    // We assume pthread_self() is async signal safe, though it's not
    // officially guaranteed.
    pthread_t currentThreadId = pthread_self();
    // NOTE: We could simply use pthread_t rather than pthread_t* for this,
    // if pthread_self() is guaranteed to return non-zero value for thread
    // ids, but there is no such guarantee. We need to distinguish if the
    // old value (value returned from __sync_val_compare_and_swap) is
    // different from the original value (in this case NULL).
    pthread_t* previousThreadId = __sync_val_compare_and_swap(
        &crashingThreadId,
        static_cast<pthread_t*>(NULL),
        &currentThreadId);

    if (previousThreadId != NULL) {
        // We've already entered the signal handler. What should we do?
        if (pthread_equal(currentThreadId, *crashingThreadId)) {
            // It looks the current thread is reentering the signal handler.
            // Something must be going wrong (maybe we are reentering by another
            // type of signal?). Kill ourself by the default signal handler.
            InvokeDefaultSignalHandler(signal);
        }
        // Another thread is dumping stuff. Let's wait until that thread
        // finishes the job and kills the process.
        while (true) {
            sleep(1);
        }
    }

    // This is the first time we enter the signal handler. We are going to
    // do some interesting stuff from here.

    TRawFormatter<16> prefix;

    // When did the crash happen?
    DumpTimeInfo();

    // Where did the crash happen?
    void *pc = GetPC(uc);

    {
        prefix.Reset();
        prefix.AppendString("PC: ");
        WriteToStderr(prefix.GetData(), prefix.GetBytesWritten());
        DumpStackFrameInfo(pc);
    }

    DumpSignalInfo(signal, si);

    // Get the stack trace (without current frame hence +1).
    std::array<void*, 99> stack; // 99 is to keep formatting. :)
    const int depth = GetStackTrace(stack.data(), stack.size(), 1);

    // Dump the stack trace.
    for (int i = 0; i < depth; ++i) {
        prefix.Reset();
        prefix.AppendNumber(i + 1, 10, 2);
        prefix.AppendString(". ");
        WriteToStderr(prefix.GetData(), prefix.GetBytesWritten());
        DumpStackFrameInfo(stack[i]);
    }

    // Okay, we have done enough, so now we can do unsafe (async signal unsafe)
    // things. The process could be terminated or hung at any time.
    NLog::TLogManager::Get()->Shutdown();

    // Kill ourself by the default signal handler.
    InvokeDefaultSignalHandler(signal);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
#endif

void InstallCrashSignalHandler()
{
#ifdef _unix_
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_flags |= SA_SIGINFO;
    sa.sa_sigaction = &CrashSignalHandler;

    for (size_t i = 0; i < ARRAY_SIZE(FailureSignals); ++i) {
        YCHECK(sigaction(FailureSignals[i].Number, &sa, NULL) == 0);
    }
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

