#include "crash_handler.h"
#include "assert.h"
#include "stack_trace.h"

#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/raw_formatter.h>

#include <yt/core/concurrency/fls.h>

#include <util/system/defaults.h>

#include <signal.h>
#include <time.h>
#include <yt/build/config.h>
#ifdef HAVE_SYS_TYPES_H
#   include <sys/types.h>
#endif
#ifdef HAVE_UNISTD_H
#   include <unistd.h>
#endif
#ifdef HAVE_UCONTEXT_H
#ifdef _linux_
#   include <ucontext.h>
#endif
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
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

//! Returns the symbol for address at given program counter.
int GetSymbolInfo(void* pc, char* buffer, int length)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

// See http://pubs.opengroup.org/onlinepubs/009695399/functions/xsh_chap02_04.html
// for a list of async signal safe functions.

// We will install the failure signal handler for these signals.
// We could use strsignal() to get signal names, but we do not use it to avoid
// introducing yet another #ifdef complication.
const struct {
    int Number;
    const char* Name;
} FailureSignals[] = {
    { SIGSEGV, "SIGSEGV" },
    { SIGILL,  "SIGILL"  },
    { SIGFPE,  "SIGFPE"  },
    { SIGABRT, "SIGABRT" },
    { SIGBUS,  "SIGBUS"  },
};

//! Returns the program counter from a signal context, NULL if unknown.
void* GetPC(void* uc)
{
    // TODO(sandello): Merge with code from Bind() internals.
#if (defined(HAVE_UCONTEXT_H) || defined(HAVE_SYS_UCONTEXT_H)) && defined(PC_FROM_UCONTEXT) && defined(_linux_)
    if (uc) {
        const auto* context = reinterpret_cast<ucontext_t*>(uc);
        return reinterpret_cast<void*>(context->PC_FROM_UCONTEXT);
    }
#endif
    return nullptr;
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
    auto timeSinceEpoch = time(nullptr);

    TRawFormatter<256> formatter;

    formatter.AppendString("*** Aborted at ");
    formatter.AppendNumber(timeSinceEpoch);
    formatter.AppendString(" (Unix time); Try \"date -d @");
    formatter.AppendNumber(timeSinceEpoch, 10);
    formatter.AppendString("\" if you are using GNU date ***\n");

    WriteToStderr(formatter.GetData(), formatter.GetBytesWritten());
}

// This variable is used for protecting CrashSignalHandler() from
// dumping stuff while another thread is doing it. Our policy is to let
// the first thread dump stuff and let other threads wait.
std::atomic<pthread_t*> CrashingThreadId;

NConcurrency::TFls<std::vector<TString>> CodicilsStack;

//! Dump codicils.
void DumpCodicils()
{
    TRawFormatter<256> formatter;

    if (!CodicilsStack->empty()) {
        formatter.Reset();
        formatter.AppendString("*** Begin codicils ***\n");
        WriteToStderr(formatter.GetData(), formatter.GetBytesWritten());

        for (const auto& data : *CodicilsStack) {
            formatter.Reset();
            formatter.AppendString(data.c_str());
            formatter.AppendString("\n");
            WriteToStderr(formatter.GetData(), formatter.GetBytesWritten());
        }

        formatter.Reset();
        formatter.AppendString("*** End codicils ***\n");
        WriteToStderr(formatter.GetData(), formatter.GetBytesWritten());
    }
}

//! Dumps information about the signal.
void DumpSignalInfo(int signal, siginfo_t* si)
{
    // Get the signal name.
    const char* name = nullptr;
    for (size_t i = 0; i < Y_ARRAY_SIZE(FailureSignals); ++i) {
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
#ifdef _unix_
    formatter.AppendString("from PID ");
    formatter.AppendNumber(si->si_pid);
    formatter.AppendString(" ");
#endif
    formatter.AppendString("***\n");

    WriteToStderr(formatter.GetData(), formatter.GetBytesWritten());
}

//! Invoke the default signal handler.
void InvokeDefaultSignalHandler(int signal)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = SIG_DFL;
    YCHECK(sigaction(signal, &sa, nullptr) == 0);

    pthread_kill(pthread_self(), signal);
}

void CrashTimeoutHandler(int signal)
{
    TRawFormatter<256> formatter;
    formatter.AppendString("*** Process hung during crash ***\n");
    WriteToStderr(formatter.GetData(), formatter.GetBytesWritten());

    _exit(1);
}

// Dumps signal and stack frame information, and invokes the default
// signal handler once our job is done.
void CrashSignalHandler(int signal, siginfo_t* si, void* uc)
{
    // All code here _MUST_ be async signal safe unless specified otherwise.

    // We assume pthread_self() is async signal safe, though it's not
    // officially guaranteed.
    auto currentThreadId = pthread_self();
    // NOTE: We could simply use pthread_t rather than pthread_t* for this,
    // if pthread_self() is guaranteed to return non-zero value for thread
    // ids, but there is no such guarantee. We need to distinguish if the
    // old value (value returned from __sync_val_compare_and_swap) is
    // different from the original value (in this case NULL).
    pthread_t* expectedCrashingThreadId = nullptr;
    if (!CrashingThreadId.compare_exchange_strong(expectedCrashingThreadId, &currentThreadId)) {
        // We've already entered the signal handler. What should we do?
        if (pthread_equal(currentThreadId, *expectedCrashingThreadId)) {
            // It looks the current thread is reentering the signal handler.
            // Something must be going wrong (maybe we are reentering by another
            // type of signal?). Kill ourself by the default signal handler.
            InvokeDefaultSignalHandler(signal);
            // If we happen to fall through here, not being killed, we will probably end up
            // running out of stack entering CrashSignalHandler over and over again.
            // Not a bad thing, after all.
        } else {
            // Another thread is dumping stuff. Let's wait until that thread
            // finishes the job and kills the process.
            while (true) {
                sleep(1);
            }
        }
    }

    // This is the first time we enter the signal handler. We are going to
    // do some interesting stuff from here.

    TRawFormatter<256> formatter;

    // When did the crash happen?
    DumpTimeInfo();

    // Dump codicils.
    DumpCodicils();

    // Where did the crash happen?
    {
        void* pc = GetPC(uc);
        formatter.Reset();
        formatter.AppendString("PC: ");
        WriteToStderr(formatter.GetData(), formatter.GetBytesWritten());
        NDetail::DumpStackFrameInfo(pc, WriteToStderr);
    }

    DumpSignalInfo(signal, si);

    DumpStackTrace(WriteToStderr);

    formatter.Reset();
    formatter.AppendString("*** Wait for logger to shut down ***\n");
    WriteToStderr(formatter.GetData(), formatter.GetBytesWritten());

    // Actually, it is not okay to hung.
    ::signal(SIGALRM, CrashTimeoutHandler);
    alarm(5);

    NLogging::TLogManager::StaticShutdown();

    formatter.Reset();
    formatter.AppendString("*** Terminate ***\n");
    WriteToStderr(formatter.GetData(), formatter.GetBytesWritten());

    // Kill ourself by the default signal handler.
    InvokeDefaultSignalHandler(signal);
}
#endif

void InstallCrashSignalHandler(TNullable<std::set<int>> signalNumbers)
{
#ifdef _unix_
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_flags |= SA_SIGINFO;
    sa.sa_sigaction = &CrashSignalHandler;

    for (size_t i = 0; i < Y_ARRAY_SIZE(FailureSignals); ++i) {
        if (!signalNumbers || signalNumbers->find(FailureSignals[i].Number) != signalNumbers->end()) {
            YCHECK(sigaction(FailureSignals[i].Number, &sa, NULL) == 0);
        }
    }
#endif
}

////////////////////////////////////////////////////////////////////////////////

void PushCodicil(const TString& data)
{
    CodicilsStack->push_back(data);
}

void PopCodicil()
{
    YCHECK(!CodicilsStack->empty());
    CodicilsStack->pop_back();
}

TCodicilGuard::TCodicilGuard()
    : Active_(false)
{ }

TCodicilGuard::TCodicilGuard(const TString& data)
    : Active_(true)
{
    PushCodicil(data);
}

TCodicilGuard::~TCodicilGuard()
{
    Release();
}

TCodicilGuard::TCodicilGuard(TCodicilGuard&& other)
    : Active_(other.Active_)
{
    other.Active_ = false;
}

TCodicilGuard& TCodicilGuard::operator=(TCodicilGuard&& other)
{
    if (this != &other) {
        Release();
        Active_ = other.Active_;
        other.Active_ = false;
    }
    return *this;
}

void TCodicilGuard::Release()
{
    if (Active_) {
        PopCodicil();
        Active_ = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

