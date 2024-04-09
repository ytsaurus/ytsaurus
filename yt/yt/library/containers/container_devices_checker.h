#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

class TContainerDevicesChecker
    : public TRefCounted
{
public:
    TContainerDevicesChecker(
        TString testDirectoryPath,
        TPortoExecutorDynamicConfigPtr config,
        IInvokerPtr invoker,
        NLogging::TLogger logger);

    void Start();

    void OnDynamicConfigChanged(const TPortoExecutorDynamicConfigPtr& newConfig);

    DEFINE_SIGNAL(void(const TError&), Check);

private:
    const TString TestDirectoryPath_;

    const TString VolumesPath_;
    const TString LayersPath_;
    const TString PortoVolumesPath_;
    const TString PortoStoragePath_;
    const TString LockPath;

    const TPortoExecutorDynamicConfigPtr Config_;
    const NLogging::TLogger Logger;
    const IInvokerPtr CheckInvoker_;
    const IPortoExecutorPtr Executor_;
    const NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;

    TString RootContainerName_;

    bool DirectoryPrepared_ = false;

    void PrepareDirectory();
    TError CreateTestContainer();
    void OnCheck();
};

DEFINE_REFCOUNTED_TYPE(TContainerDevicesChecker)

////////////////////////////////////////////////////////////////////////////////

TContainerDevicesCheckerPtr CreateContainerDevicesChecker(
    TString testDirectoryPath,
    TPortoExecutorDynamicConfigPtr config,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
