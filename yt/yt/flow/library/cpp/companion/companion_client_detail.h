#pragma once

#include "public.h"

#include "companion_client.h"

#include <yt/yt/flow/library/cpp/companion/companion_proxy.h>

#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/config.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

class TCompanionClient
    : public ICompanionClient
{
public:
    TCompanionClient(
        const std::string& address,
        const TDuration& timeout,
        const TExponentialBackoffOptions& backoffOptions,
        const IStatusProfilerPtr& statusProfiler);

    ~TCompanionClient() override = default;

    TCompanionResponsePtr DoProcessWithCompanionSync(
        const TCompanionProcessRequestPtr& companionRequest,
        const IExternalPerformanceMetricsReporterPtr& reporter) override;

    TCompanionInfoPtr GetCompanionInfo() override;

    TCompanionPutJobResponsePtr PutJob(
        const TCompanionPutJobRequestPtr& putJobRequest,
        const IExternalPerformanceMetricsReporterPtr& reporter) override;

private:
    template <typename TResponse>
    TFuture<TResponse> ExecuteWithRetry(
        const TCallback<TFuture<TResponse>()>& callback,
        const std::string& operationName);

private:
    const TDuration Timeout_;
    const TExponentialBackoffOptions BackoffOptions_;
    const NLogging::TLogger Logger;
    const TCompanionProxy CompanionProxy_;
    const IStatusProfilerPtr StatusProfiler_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
