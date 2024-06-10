#pragma once

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/profiling/timing.h>

#include <time.h>

namespace NRoren
{

////////////////////////////////////////////////////////////////////////////////

using TClockId = clockid_t;

class TAccountCpuTimeInvoker
    : public NYT::IInvoker
{
private:
    class TFiberCpuTimer
        : private NYT::NConcurrency::TContextSwitchGuard
    {
    public:
        struct TSharedState
            : NYT::TRefCounted
        {
            std::atomic<i64> DurationNSec_ = 0;
            TClockId ClockId = CLOCK_THREAD_CPUTIME_ID;
        };

        TFiberCpuTimer(NYT::TIntrusivePtr<TSharedState> sharedState);
        ~TFiberCpuTimer();

    private:
        void Start();
        void Stop();

    private:
        NYT::TIntrusivePtr<TSharedState> SharedState_;
        bool Active_ = true;
        i64 StartTime_;
    };

    NYT::TClosure MakeWrappedCallback(NYT::TClosure callback);

public:
    TAccountCpuTimeInvoker(NYT::IInvokerPtr invoker);
    TDuration GetCpuTime() const;

    using IInvoker::TWaitTimeObserver;
    void Invoke(NYT::TClosure callback) override;
    void Invoke(NYT::TMutableRange<NYT::TClosure> callbacks) override;
    NYT::NThreading::TThreadId GetThreadId() const override;
    bool CheckAffinity(const NYT::IInvokerPtr& invoker) const override;
    bool IsSerialized() const override;
    void RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver) override;

private:
    NYT::IInvokerPtr Invoker_;
    NYT::TIntrusivePtr<TFiberCpuTimer::TSharedState> SharedState_;
};

DEFINE_REFCOUNTED_TYPE(TAccountCpuTimeInvoker)
using TAccountCpuTimeInvokerPtr = NYT::TIntrusivePtr<TAccountCpuTimeInvoker>;

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren
