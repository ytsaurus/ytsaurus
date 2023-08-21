#pragma once

#include "private.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/actions/callback.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TMemoryWatchdog
    : public TRefCounted
{
public:
    TMemoryWatchdog(
        TMemoryWatchdogConfigPtr config,
        TCallback<void()> exitCallback,
        TCallback<void()> interruptCallback);

    void Start();
    void Stop();

private:
    const TMemoryWatchdogConfigPtr Config_;
    const TCallback<void()> ExitCallback_;
    const TCallback<void()> InterruptCallback_;
    NConcurrency::TActionQueuePtr ActionQueue_;
    const IInvokerPtr Invoker_;
    const NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    std::deque<std::pair<TInstant, size_t>> WindowRssValues_;

    void CheckMemoryUsage();
    void CheckRss(size_t rss);
    void CheckMinimumWindowRss(size_t minimumWindowRss);
    void DumpRefCountedTracker();
};

DEFINE_REFCOUNTED_TYPE(TMemoryWatchdog)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
