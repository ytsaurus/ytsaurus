#include "config.h"

namespace NYT::NTransactionClient {

using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TPingBatcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
    registrar.Parameter("batch_period", &TThis::BatchPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("batch_size", &TThis::BatchSize)
        .Default(10'000);
}

////////////////////////////////////////////////////////////////////////////////

void TTransactionManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("default_ping_period", &TThis::DefaultPingPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("default_transaction_timeout", &TThis::DefaultTransactionTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("use_cypress_transaction_service", &TThis::UseCypressTransactionService)
        .Default(false);
    registrar.Parameter("ping_batcher", &TThis::PingBatcher)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->RetryAttempts = 100;
        config->RetryTimeout = TDuration::Minutes(3);
    });

    registrar.Postprocessor([] (TThis* config) {
        if (config->DefaultTransactionTimeout <= config->DefaultPingPeriod) {
            THROW_ERROR_EXCEPTION("\"default_transaction_timeout\" must be greater than \"default_ping_period\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TClockManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("clock_cluster_tag", &TThis::ClockClusterTag)
        .Default(InvalidCellTag);
}

TClockManagerConfigPtr TClockManagerConfig::ApplyDynamic(
    const TDynamicClockManagerConfigPtr& dynamicConfig) const
{
    auto mergedConfig = New<TClockManagerConfig>();
    UpdateYsonStructField(mergedConfig->ClockClusterTag, dynamicConfig->ClockClusterTag);
    mergedConfig->Postprocess();
    return mergedConfig;
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicClockManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("clock_cluster_tag", &TThis::ClockClusterTag)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
