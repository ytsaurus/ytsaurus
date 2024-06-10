#include "config.h"

namespace NRoren {

DEFINE_REFCOUNTED_TYPE(TComputationConfig);

void TComputationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("stateless_shard_processor_config", &TThis::StatelessShardProcessorConfig).Optional();
    registrar.Parameter("consuming_system_config", &TThis::ConsumingSystemConfig).Optional();
    registrar.Parameter("fallback_shard_processor_config", &TThis::FallbackShardProcessorConfig).Optional();
    registrar.Parameter("throttler_quota", &TThis::ThrottlerQuota).Optional();
    registrar.Parameter("input", &TThis::Input).Optional();
    registrar.Parameter("inputs", &TThis::Inputs).Optional();
    registrar.Parameter("transaction_keeper_group", &TThis::TransactionKeeperGroup).Default("");
    registrar.Postprocessor([] (TThis* self) {
        THROW_ERROR_EXCEPTION_IF(!self->Inputs.empty() + self->Input.has_value() > 1, "Input and Inputs are alternatives"); // May be zero for default param.
        self->StatelessShardProcessorConfig.DiscardUnknownFields();
        self->ConsumingSystemConfig.DiscardUnknownFields();
        if (self->FallbackShardProcessorConfig) {
            self->FallbackShardProcessorConfig->DiscardUnknownFields();
        }
        if (self->ThrottlerQuota) {
            self->ThrottlerQuota->DiscardUnknownFields();
        }
        if (self->Input) {
            self->Input->DiscardUnknownFields();
        }
        for (auto& [_, input] : self->Inputs) {
            input.DiscardUnknownFields();
        }
    });
}

DEFINE_REFCOUNTED_TYPE(TDestinationConfig);

void TDestinationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("swift", &TThis::SwiftWriter).Optional();
    registrar.Parameter("yt_queue", &TThis::YtQueueWriter).Optional();
    registrar.Parameter("shard_count", &TThis::ShardCount).Default(0);
    registrar.Postprocessor([] (TThis* self) {
        THROW_ERROR_EXCEPTION_IF(self->SwiftWriter.has_value() + self->YtQueueWriter.has_value() != 1, "Swift and YtQueue are alternatives");
        if (self->SwiftWriter) {
            self->SwiftWriter->DiscardUnknownFields();
        }
        if (self->YtQueueWriter) {
            self->YtQueueWriter->DiscardUnknownFields();
        }
    });
}

DEFINE_REFCOUNTED_TYPE(TBigRtConfig);

void TBigRtConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("main_yt_cluster", &TThis::MainYtCluster).Optional();
    registrar.Parameter("main_yt_path", &TThis::MainYtPath).Optional();
    registrar.Parameter("default_computation_settings", &TThis::DefaultComputationSettings).Optional();
    registrar.Parameter("computations", &TThis::Computations);
    registrar.Parameter("destinations", &TThis::Destinations).Optional();
    registrar.Parameter("max_in_flight_bytes", &TThis::MaxInFlightBytes).Default(67108864);
    registrar.Parameter("enable_wait_process_started", &TThis::EnableWaitProcessStarted).Default(false);
    registrar.Parameter("auto_launch_balancer_internal_master", &TThis::AutoLaunchBalancerInternalMaster).Default(false);
    registrar.Parameter("use_processor_v3", &TThis::UseProcessorV3).Default(false);
    registrar.Parameter("legacy_thread_count", &TThis::LegacyThreadCount).Optional();
    registrar.Parameter("shared_transaction_period", &TThis::SharedTransactionPeriod).Default(TDuration::Zero());
}

} // namespace NRoren
