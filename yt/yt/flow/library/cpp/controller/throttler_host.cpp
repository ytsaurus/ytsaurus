#include "throttler_host.h"

#include "private.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/distributed_throttler/config.h>
#include <yt/yt/flow/library/cpp/distributed_throttler/server.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NFlow::NController {

using namespace NDistributedThrottler;

////////////////////////////////////////////////////////////////////////////////

namespace {

TDistributedThrottlerServiceConfigPtr BuildServiceConfig(
    const TDynamicPipelineSpecPtr& dynamicSpec)
{
    auto config = New<TDistributedThrottlerServiceConfig>();
    for (const auto& [name, throttlerSpec] : dynamicSpec->Throttlers) {
        config->Throttlers.emplace(name.Underlying(), throttlerSpec->BuildThroughputConfig());
    }
    return config;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TThrottlerHost
    : public IThrottlerHost
{
public:
    TThrottlerHost(
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler)
        : Service_(CreateDistributedThrottlerService(
            New<TDistributedThrottlerServiceConfig>(),
            std::move(invoker),
            std::move(logger),
            std::move(profiler)))
    { }

    void Reconfigure(const TDynamicPipelineSpecPtr& dynamicSpec) override
    {
        Service_->Reconfigure(BuildServiceConfig(dynamicSpec));
    }

    NRpc::IServicePtr GetRpcService() override
    {
        return Service_->GetRpcService();
    }

private:
    const IDistributedThrottlerServicePtr Service_;
};

////////////////////////////////////////////////////////////////////////////////

IThrottlerHostPtr CreateThrottlerHost(
    IInvokerPtr invoker,
    NYPath::TRichYPath pipelinePath)
{
    return New<TThrottlerHost>(
        std::move(invoker),
        ControllerLogger(),
        ControllerProfiler()
            .WithDefaultDisabled()
            .WithRequiredTag("pipeline_path", pipelinePath.GetPath())
            .WithRequiredTag("pipeline_cluster", *pipelinePath.GetCluster())
            .WithPrefix("/distributed_throttler"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
