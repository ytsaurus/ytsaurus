#pragma once

#include "bootstrap.h"

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/logging/log.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TMemoryWatchdog
    : public TRefCounted
{
public:
    TMemoryWatchdog(TMemoryWatchdogConfigPtr config, TBootstrap* bootstrap);

    void UpdateConfig(TMemoryWatchdogConfigPtr config);

private:
    TBootstrap* const Bootstrap_;
    TMemoryWatchdogConfigPtr Config_;
    NConcurrency::TPeriodicExecutorPtr MemoryCheckExecutor_;
    const NLogging::TLogger Logger;

    void DoCheckMemoryUsage();
};

DEFINE_REFCOUNTED_TYPE(TMemoryWatchdog)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
