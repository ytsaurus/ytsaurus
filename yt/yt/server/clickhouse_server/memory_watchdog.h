#pragma once

#include "private.h"

#include <yt/core/concurrency/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TMemoryWatchdog
    : public TRefCounted
{
public:
    TMemoryWatchdog(TMemoryWatchdogConfigPtr config, TCallback<void()> exitCallback, TCallback<void()> interruptCallback);

    void Start();
    void Stop();

private:
    TMemoryWatchdogConfigPtr Config_;
    TCallback<void()> ExitCallback_;
    TCallback<void()> InterruptCallback_;
    NConcurrency::TActionQueuePtr ActionQueue_;
    IInvokerPtr Invoker_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    std::deque<std::pair<TInstant, size_t>> WindowRssValues_;

    void CheckMemoryUsage();
    [[noreturn]] void KillSelf(TString reason, bool graceful);
};

DEFINE_REFCOUNTED_TYPE(TMemoryWatchdog)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
