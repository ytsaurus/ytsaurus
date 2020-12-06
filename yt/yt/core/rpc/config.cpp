#include "config.h"

namespace NYT::NRpc {

using namespace NBus;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const int TServiceConfig::DefaultAuthenticationQueueSizeLimit = 10000;
const TDuration TServiceConfig::DefaultPendingPayloadsTimeout = TDuration::Seconds(30);

////////////////////////////////////////////////////////////////////////////////

const bool TMethodConfig::DefaultHeavy = false;
const int TMethodConfig::DefaultQueueSizeLimit = 10000;
const int TMethodConfig::DefaultConcurrencyLimit = 1000;
const NLogging::ELogLevel TMethodConfig::DefaultLogLevel = NLogging::ELogLevel::Debug;
const TDuration TMethodConfig::DefaultLoggingSuppressionTimeout = TDuration::Zero();
const TThroughputThrottlerConfigPtr TMethodConfig::DefaultLoggingSuppressionFailedRequestThrottler =
    New<TThroughputThrottlerConfig>(1000);

////////////////////////////////////////////////////////////////////////////////

TDispatcherConfig::TDispatcherConfig()
{
    RegisterParameter("heavy_pool_size", HeavyPoolSize)
        .Default(DefaultHeavyPoolSize)
        .GreaterThan(0);
    RegisterParameter("compression_pool_size", CompressionPoolSize)
        .Default(DefaultCompressionPoolSize)
        .GreaterThan(0);
    RegisterParameter("multiplexing_bands", MultiplexingBands)
        .Default();
}

TDispatcherConfigPtr TDispatcherConfig::ApplyDynamic(const TDispatcherDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = New<TDispatcherConfig>();
    mergedConfig->HeavyPoolSize = dynamicConfig->HeavyPoolSize.value_or(HeavyPoolSize);
    mergedConfig->CompressionPoolSize = dynamicConfig->CompressionPoolSize.value_or(CompressionPoolSize);
    mergedConfig->MultiplexingBands = dynamicConfig->MultiplexingBands.value_or(MultiplexingBands);
    return mergedConfig;
}

////////////////////////////////////////////////////////////////////////////////

TDispatcherDynamicConfig::TDispatcherDynamicConfig()
{
    RegisterParameter("heavy_pool_size", HeavyPoolSize)
        .Optional()
        .GreaterThan(0);
    RegisterParameter("compression_pool_size", CompressionPoolSize)
        .Optional()
        .GreaterThan(0);
    RegisterParameter("multiplexing_bands", MultiplexingBands)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
