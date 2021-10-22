#pragma once

#include "spinlock.h"
#include "event_count.h"

#include <yt/yt/core/misc/shutdown.h>

#include <util/system/thread.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! A shutdown-aware thread wrapper.
class TThread
    : public TRefCounted
{
public:
    TThread(
        TString threadName,
        int shutdownPriority = 0);
    ~TThread();

    //! Ensures the thread is started.
    /*!
     *  Also invokes start hooks (in the caller's thread).
     *  Safe to call multiple times. Fast on fastpath.
     *  Returns true if the thread has been indeed started.
     */
    bool Start();

    //! Ensures the thread is stopped.
    /*!
     *  Safe to call multiple times.
     *  Also invokes stop hooks (in the caller's thread).
     */
    void Stop();

    bool IsStarted() const;
    bool IsStopping() const;

    TThreadId GetThreadId() const;

protected:
    virtual void StartPrologue();
    virtual void StartEpilogue();
    virtual void StopPrologue();
    virtual void StopEpilogue();

    virtual void ThreadMain() = 0;

private:
    const TString ThreadName_;
    const int ShutdownPriority_;

    const TThreadId UniqueThreadId_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    std::atomic<bool> Started_ = false;
    std::atomic<bool> Stopping_ = false;
    TShutdownCookie ShutdownCookie_;

    TEvent StartedEvent_;
    TEvent StoppedEvent_;

    TThreadId ThreadId_ = InvalidThreadId;
    ::TThread UnderlyingThread_;

    bool StartSlow();

    bool CanWaitForThreadShutdown() const;

    static void* StaticThreadMainTrampoline(void* opaque);
    void ThreadMainTrampoline();

};

DEFINE_REFCOUNTED_TYPE(TThread)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTY::NConcurrency

#define THREAD_INL_H_
#include "thread-inl.h"
#undef THREAD_INL_H_
