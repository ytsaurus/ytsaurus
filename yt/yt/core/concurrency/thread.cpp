#include "thread.h"

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/misc/proc.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

static thread_local TThreadId CurrentUniqueThreadId;
static std::atomic<TThreadId> UniqueThreadIdGenerator;

TThread::TThread(
    TString threadName,
    int shutdownPriority)
    : ThreadName_(std::move(threadName))
    , ShutdownPriority_(shutdownPriority)
    , UniqueThreadId_(++UniqueThreadIdGenerator)
    , UnderlyingThread_(&StaticThreadMainTrampoline, this)
{ }

TThread::~TThread()
{
    Stop();
}

TThreadId TThread::GetThreadId() const
{
    return ThreadId_;
}

bool TThread::StartSlow()
{
    auto guard = Guard(SpinLock_);

    if (Started_.load()) {
        return !Stopping_.load();
    }

    if (Stopping_.load()) {
        // Stopped without being started.
        return false;
    }

    ShutdownCookie_ = RegisterShutdownCallback(
        Format("Thread(%v)", ThreadName_),
        BIND(&TThread::Stop, MakeWeak(this)),
        ShutdownPriority_);
    if (!ShutdownCookie_) {
        Stopping_ = true;
        return false;
    }

    if (auto* logFile = GetShutdownLogFile()) {
        ::fprintf(logFile, "*** Starting thread (ThreadName: %s)\n",
            ThreadName_.c_str());
    }

    StartPrologue();

    try {
        UnderlyingThread_.Start();
    } catch (const std::exception& ex) {
        fprintf(stderr, "*** Error starting thread (ThreadName: %s)\n*** %s\n",
            ThreadName_.c_str(),
            ex.what());
        YT_ABORT();
    }

    Started_ = true;

    StartedEvent_.Wait();

    StartEpilogue();

    if (auto* logFile = GetShutdownLogFile()) {
        ::fprintf(logFile, "*** Thread started (ThreadName: %s, ThreadId: %" PRISZT ")\n",
            ThreadName_.c_str(),
            ThreadId_);
    }

    return true;
}

bool TThread::CanWaitForThreadShutdown() const
{
    return
        CurrentUniqueThreadId != UniqueThreadId_ &&
        GetShutdownThreadId() != ThreadId_;
}

void TThread::Stop()
{
    {
        auto guard = Guard(SpinLock_);
        auto alreadyStopping = Stopping_.exchange(true);
        if (!Started_) {
            return;
        }
        if (alreadyStopping) {
            guard.Release();
            // Avoid deadlock.
            if (CanWaitForThreadShutdown()) {
                if (auto* logFile = GetShutdownLogFile()) {
                    ::fprintf(logFile, "*** Waiting for an already stopping thread to finish (ThreadName: %s, ThreadId: %" PRISZT ", WaiterThreadId: %" PRISZT ")\n",
                        ThreadName_.c_str(),
                        ThreadId_,
                        GetCurrentThreadId());
                }
                StoppedEvent_.Wait();
            } else {
                if (auto* logFile = GetShutdownLogFile()) {
                    ::fprintf(logFile, "*** Cannot wait for an already stopping thread to finish (ThreadName: %s, ThreadId: %" PRISZT ", WaiterThreadId: %" PRISZT ")\n",
                        ThreadName_.c_str(),
                        ThreadId_,
                        GetCurrentThreadId());
                }
            }
            return;
        }
    }

    if (auto* logFile = GetShutdownLogFile()) {
        ::fprintf(logFile, "*** Stopping thread (ThreadName: %s, ThreadId: %" PRISZT ", RequesterThreadId: %" PRISZT ")\n",
            ThreadName_.c_str(),
            ThreadId_,
            GetCurrentThreadId());
    }

    StopPrologue();

    // Avoid deadlock.
    if (CanWaitForThreadShutdown()) {
        if (auto* logFile = GetShutdownLogFile()) {
            ::fprintf(logFile, "*** Waiting for thread to stop (ThreadName: %s, ThreadId: %" PRISZT ", RequesterThreadId: %" PRISZT ")\n",
                ThreadName_.c_str(),
                ThreadId_,
                GetCurrentThreadId());
        }
        UnderlyingThread_.Join();
    } else {
        if (auto* logFile = GetShutdownLogFile()) {
            ::fprintf(logFile, "*** Cannot wait for thread to stop; detaching (ThreadName: %s, ThreadId: %" PRISZT ", RequesterThreadId: %" PRISZT ")\n",
                ThreadName_.c_str(),
                ThreadId_,
                GetCurrentThreadId());
        }
        UnderlyingThread_.Detach();
    }

    StopEpilogue();

    if (auto* logFile = GetShutdownLogFile()) {
        ::fprintf(logFile, "*** Thread stopped (ThreadName: %s, ThreadId: %" PRISZT ", RequesterThreadId: %" PRISZT ")\n",
            ThreadName_.c_str(),
            ThreadId_,
            GetCurrentThreadId());
    }
}

void* TThread::StaticThreadMainTrampoline(void* opaque)
{
    reinterpret_cast<TThread*>(opaque)->ThreadMainTrampoline();
    return nullptr;
}

void TThread::ThreadMainTrampoline()
{
    auto this_ = MakeStrong(this);

    ::TThread::SetCurrentThreadName(ThreadName_.c_str());

    ThreadId_ = GetCurrentThreadId();
    CurrentUniqueThreadId = UniqueThreadId_;

    StartedEvent_.NotifyAll();

    class TExitInterceptor
    {
    public:
        ~TExitInterceptor()
        {
            if (Armed_ && !std::uncaught_exception()) {
                if (auto* logFile = GetShutdownLogFile()) {
                    ::fprintf(logFile, "Thread exit interceptor triggered (ThreadId: %" PRISZT ")\n",
                        GetCurrentThreadId());
                }
                Shutdown();
            }
        }

        void Disarm()
        {
            Armed_ = false;
        }

    private:
        bool Armed_ = false;
    };

    static thread_local TExitInterceptor Interceptor;

    ThreadMain();

    Interceptor.Disarm();

    StoppedEvent_.NotifyAll();
}

void TThread::StartPrologue()
{ }

void TThread::StartEpilogue()
{ }

void TThread::StopPrologue()
{ }

void TThread::StopEpilogue()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

