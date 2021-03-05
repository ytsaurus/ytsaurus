#pragma once

#include "public.h"
#include "event_count.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/misc/common.h>
#include <yt/yt/core/misc/shutdownable.h>

#include <util/system/thread.h>
#include <util/system/sigset.h>

#include <yt/yt/library/profiling/tag.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerThreadBase
    : public virtual TRefCounted
    , public IShutdownable
{
public:
    ~TSchedulerThreadBase();

    void Start();
    virtual void Shutdown() override;

    TThreadId GetId() const;

    bool IsStarted() const;
    bool IsShutdown() const;

protected:
    const std::shared_ptr<TEventCount> CallbackEventCount_;
    const TString ThreadName_;

    TSchedulerThreadBase(
        std::shared_ptr<TEventCount> callbackEventCount,
        const TString& threadName);

    virtual void OnStart();
    virtual void BeforeShutdown();
    virtual void AfterShutdown();

    virtual void OnThreadStart();
    virtual void OnThreadShutdown();

    virtual bool OnLoop(TEventCount::TCookie* cookie) = 0;

private:
    std::atomic<ui64> Epoch_ = 0;
    static constexpr ui64 StartingEpochMask = 0x1;
    static constexpr ui64 StoppingEpochMask = 0x2;

    TEvent ThreadStartedEvent_;
    TEvent ThreadShutdownEvent_;

    TThreadId ThreadId_ = InvalidThreadId;
    TThread Thread_;

    static void* ThreadTrampoline(void* opaque);
    void ThreadMain();
};

DEFINE_REFCOUNTED_TYPE(TSchedulerThreadBase)

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency
