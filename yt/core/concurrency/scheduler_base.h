#pragma once

#include "scheduler.h"

#include <yt/core/misc/common.h>
#include <yt/core/misc/shutdownable.h>

#include <util/system/thread.h>
#include <util/system/sigset.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerThreadBase
    : public virtual TRefCounted
    , public IShutdownable
{
public:
    ~TSchedulerThreadBase();

    // TODO: Start in constructor?
    void Start();

    virtual void Shutdown() override;

    virtual void OnStart()
    { }

    virtual void BeforeShutdown()
    { }

    virtual void AfterShutdown()
    { }

    virtual void OnThreadStart()
    {
    #ifdef _unix_
        // Set empty sigmask for all threads.
        sigset_t sigset;
        SigEmptySet(&sigset);
        SigProcMask(SIG_SETMASK, &sigset, nullptr);
    #endif
    }

    virtual void OnThreadShutdown()
    { }

    TThreadId GetId() const
    {
        return ThreadId_;
    }

    bool IsStarted() const
    {
        return Epoch_.load(std::memory_order_relaxed) & StartedEpochMask;
    }

    bool IsShutdown() const
    {
        return Epoch_.load(std::memory_order_relaxed) & ShutdownEpochMask;
    }

protected:
    TSchedulerThreadBase(
        std::shared_ptr<TEventCount> callbackEventCount,
        const TString& threadName,
        bool enableLogging)
        : CallbackEventCount_(std::move(callbackEventCount))
        , ThreadName_(threadName)
        , EnableLogging_(enableLogging)
        , Thread_(ThreadTrampoline, (void*) this)
    { }

    virtual bool OnLoop(TEventCount::TCookie* cookie) = 0;

    static void* ThreadTrampoline(void* opaque)
    {
        static_cast<TSchedulerThreadBase*>(opaque)->ThreadMain();
        return nullptr;
    }

    void ThreadMain();

    std::atomic<ui64> Epoch_ = {0};
    static constexpr ui64 StartedEpochMask = 0x1;
    static constexpr ui64 ShutdownEpochMask = 0x2;
    TEvent ThreadStartedEvent_;
    TEvent ThreadShutdownEvent_;

    const std::shared_ptr<TEventCount> CallbackEventCount_;
    const TString ThreadName_;
    const bool EnableLogging_;

    TThreadId ThreadId_ = InvalidThreadId;
    TThread Thread_;

};

DEFINE_REFCOUNTED_TYPE(TSchedulerThreadBase)

class TFiberReusingAdapter
    : public TSchedulerThreadBase
{
public:
    TFiberReusingAdapter(
        std::shared_ptr<TEventCount> callbackEventCount,
        const TString& threadName,
        bool enableLogging = true)
        : TSchedulerThreadBase(
            callbackEventCount,
            threadName,
            enableLogging)
    { }

    TFiberReusingAdapter(
        std::shared_ptr<TEventCount> callbackEventCount,
        const TString& threadName,
        const NProfiling::TTagIdList&,
        bool enableLogging = true,
        bool enableProfiling = true)
        : TFiberReusingAdapter(
            std::move(callbackEventCount),
            std::move(threadName),
            enableLogging)
    {
        Y_UNUSED(enableProfiling);
    }

    void CancelWait();

    void PrepareWait();

    void Wait();

    virtual bool OnLoop(TEventCount::TCookie* cookie) override;

    virtual TClosure BeginExecute() = 0;

    virtual void EndExecute() = 0;

private:
    std::optional<TEventCount::TCookie> Cookie_;

};

/////////////////////////////////////////////////////////////////////////////

// Temporary adapters.
using TSchedulerThread = TFiberReusingAdapter;

DECLARE_REFCOUNTED_TYPE(TSchedulerThread)
DEFINE_REFCOUNTED_TYPE(TSchedulerThread)

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
