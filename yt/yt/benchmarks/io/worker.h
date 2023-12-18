#pragma once

#include "iotest.h"
#include "configuration.h"
#include "statistics.h"

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

struct IWorker
    : public TRefCounted
{
    virtual void Run() = 0;

    virtual void Join() = 0;

    virtual TStatistics Statistics() = 0;
};

DECLARE_REFCOUNTED_STRUCT(IWorker)
DEFINE_REFCOUNTED_TYPE(IWorker)

////////////////////////////////////////////////////////////////////////////////

IWorkerPtr CreateWorker(TConfigurationPtr configuration, int threadIndex);

////////////////////////////////////////////////////////////////////////////////

struct IWatcher
    : public IWorker
{
    virtual void Stop() = 0;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IWatcher)
DEFINE_REFCOUNTED_TYPE(IWatcher)

IWatcherPtr CreateRusageWatcher(TInstant start);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest


