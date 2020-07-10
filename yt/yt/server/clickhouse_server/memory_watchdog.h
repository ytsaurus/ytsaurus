#pragma once

#include "private.h"

#include <yt/core/concurrency/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TMemoryWatchdog
    : public TRefCounted
{
public:
    TMemoryWatchdog(TMemoryWatchdogConfigPtr config, TCallback<void()> exitCallback);

    void Start();
    void Stop();

private:
    TMemoryWatchdogConfigPtr Config_;
    TCallback<void()> ExitCallback_;
    NConcurrency::TActionQueuePtr ActionQueue_;
    IInvokerPtr Invoker_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    std::deque<std::pair<TInstant, size_t>> WindowRssValues_;

    void CheckMemoryUsage();
    [[noreturn]] void KillSelf(TString reason);
};

DEFINE_REFCOUNTED_TYPE(TMemoryWatchdog)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
