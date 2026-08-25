#include "config.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow::NDistributedThrottler {

////////////////////////////////////////////////////////////////////////////////

void TDistributedThrottlerBucketConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("throttler", &TThis::Throttler)
        .DefaultNew();
    registrar.Parameter("class_weights", &TThis::ClassWeights)
        .Default();
    registrar.Parameter("max_grant_amount", &TThis::MaxGrantAmount)
        .Default()
        .GreaterThan(0);

    // Configs built directly (bypassing the pipeline spec) must uphold the
    // same invariants as TDynamicThrottlerSpec; both call the shared helpers.
    registrar.Postprocessor([] (TThis* config) {
        for (const auto& [classId, weight] : config->ClassWeights) {
            try {
                ValidateQuotaClassName(classId);
                ValidateQuotaClassWeight(weight);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Invalid quota class %Qv", classId)
                    .With(ex);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDistributedThrottlerServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("throttlers", &TThis::Throttlers)
        .Default();
    registrar.Parameter("queue_timeout", &TThis::QueueTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("drain_period", &TThis::DrainPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("response_keeper", &TThis::ResponseKeeper)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TDistributedThrottlerClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("server_address", &TThis::ServerAddress)
        .Default();
    registrar.Parameter("throttler_name", &TThis::ThrottlerName)
        .Default();
    registrar.Parameter("client_id", &TThis::ClientId)
        .Default();
    registrar.Parameter("prefetching_config", &TThis::PrefetchingConfig)
        .DefaultNew();
    registrar.Parameter("retrying_channel", &TThis::RetryingChannel)
        .DefaultNew();
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDistributedThrottler
