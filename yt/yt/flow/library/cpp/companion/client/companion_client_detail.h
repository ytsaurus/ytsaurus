#pragma once

#include "public.h"

#include "companion_client.h"

#include <yt/yt/flow/library/cpp/companion/client/companion_proxy.h>

#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/config.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/system/spin_lock.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

class TCompanionClient
    : public ICompanionClient
{
public:
    //! |profiler| receives the client's retry counters.
    TCompanionClient(
        const std::string& address,
        const TDuration& timeout,
        const TExponentialBackoffOptions& backoffOptions,
        const IStatusProfilerPtr& statusProfiler,
        NProfiling::TProfiler profiler = {});

    ~TCompanionClient() override = default;

    TCompanionResponsePtr DoProcessWithCompanionSync(
        const TCompanionProcessRequestPtr& companionRequest,
        const IExternalPerformanceMetricsReporterPtr& reporter) override;

    TCompanionInfoPtr GetCompanionInfo() override;

    TCompanionPutJobResponsePtr PutJob(
        const TCompanionPutJobRequestPtr& putJobRequest,
        const IExternalPerformanceMetricsReporterPtr& reporter) override;

    TFuture<void> RemoveJob(const TJobId& jobId) override;

    TFuture<TCompanionJobList> ListJobs() override;

    TFuture<TCompanionResourceExecuteResponsePtr> ResourceExecute(
        const TResourceId& resourceId,
        ECompanionResourceCommand command,
        const NYson::TYsonString& argument) override;

private:
    template <typename TResponse>
    TFuture<TResponse> ExecuteWithRetry(
        const TCallback<TFuture<TResponse>()>& callback,
        const std::string& operationName);

    void ProfileRetry(const std::string& operationName);

private:
    const TDuration Timeout_;
    const TExponentialBackoffOptions BackoffOptions_;
    const NLogging::TLogger Logger;
    const TCompanionProxy CompanionProxy_;
    const IStatusProfilerPtr StatusProfiler_;
    const NProfiling::TProfiler Profiler_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, RetryCountersLock_);
    //! Keyed by operation name.
    THashMap<std::string, NProfiling::TCounter> RetryCounters_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
