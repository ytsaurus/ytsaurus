#include "config.h"

namespace NYT::NRpc {

using namespace NBus;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

THistogramExponentialBounds::THistogramExponentialBounds()
{
    RegisterParameter("min", Min).Default(TDuration::Zero());
    RegisterParameter("max", Max).Default(TDuration::Seconds(2));
}

////////////////////////////////////////////////////////////////////////////////

THistogramConfig::THistogramConfig()
{
    RegisterParameter("exponential_bounds", ExponentialBounds).Optional();
    RegisterParameter("custom_bounds", CustomBounds).Optional();
}

////////////////////////////////////////////////////////////////////////////////

TServiceCommonConfig::TServiceCommonConfig()
{
    RegisterParameter("enable_per_user_profiling", EnablePerUserProfiling)
        .Default(false);
    RegisterParameter("histogram_timer_profiling", HistogramTimerProfiling)
        .Default(nullptr);
    RegisterParameter("tracing_mode", TracingMode)
        .Default(ERequestTracingMode::Enable);
}

////////////////////////////////////////////////////////////////////////////////

TServerConfig::TServerConfig()
{
    RegisterParameter("services", Services)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TServiceConfig::TServiceConfig()
{
    RegisterParameter("enable_per_user_profiling", EnablePerUserProfiling)
        .Optional();
    RegisterParameter("histogram_config", HistogramTimerProfiling)
        .Default(nullptr);
    RegisterParameter("tracing_mode", TracingMode)
        .Optional();
    RegisterParameter("methods", Methods)
        .Optional();
    RegisterParameter("authentication_queue_size_limit", AuthenticationQueueSizeLimit)
        .Alias("max_authentication_queue_size")
        .Optional();
    RegisterParameter("pending_payloads_timeout", PendingPayloadsTimeout)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TMethodConfig::TMethodConfig()
{
    RegisterParameter("heavy", Heavy)
        .Optional();
    RegisterParameter("queue_size_limit", QueueSizeLimit)
        .Alias("max_queue_size")
        .Optional();
    RegisterParameter("concurrency_limit", ConcurrencyLimit)
        .Alias("max_concurrency")
        .Optional();
    RegisterParameter("log_level", LogLevel)
        .Optional();
    RegisterParameter("request_bytes_throttler", RequestBytesThrottler)
        .Default();
    RegisterParameter("logging_suppression_timeout", LoggingSuppressionTimeout)
        .Optional();
    RegisterParameter("logging_suppression_failed_request_throttler", LoggingSuppressionFailedRequestThrottler)
        .Optional();
    RegisterParameter("tracing_mode", TracingMode)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TRetryingChannelConfig::TRetryingChannelConfig()
{
    RegisterParameter("retry_backoff_time", RetryBackoffTime)
        .Default(TDuration::Seconds(3));
    RegisterParameter("retry_attempts", RetryAttempts)
        .GreaterThanOrEqual(1)
        .Default(10);
    RegisterParameter("retry_timeout", RetryTimeout)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TBalancingChannelConfigBase::TBalancingChannelConfigBase()
{
    RegisterParameter("discover_timeout", DiscoverTimeout)
        .Default(TDuration::Seconds(15));
    RegisterParameter("acknowledgement_timeout", AcknowledgementTimeout)
        .Default(TDuration::Seconds(15));
    RegisterParameter("rediscover_period", RediscoverPeriod)
        .Default(TDuration::Seconds(60));
    RegisterParameter("rediscover_splay", RediscoverSplay)
        .Default(TDuration::Seconds(15));
    RegisterParameter("hard_backoff_time", HardBackoffTime)
        .Default(TDuration::Seconds(60));
    RegisterParameter("soft_backoff_time", SoftBackoffTime)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

TDynamicChannelPoolConfig::TDynamicChannelPoolConfig()
{
    RegisterParameter("max_concurrent_discover_requests", MaxConcurrentDiscoverRequests)
        .GreaterThan(0)
        .Default(10);
    RegisterParameter("hashes_per_peer", HashesPerPeer)
        .GreaterThan(0)
        .Default(10);
    RegisterParameter("max_peer_count", MaxPeerCount)
        .GreaterThan(1)
        .Default(100);
    RegisterParameter("random_peer_eviction_period", RandomPeerEvictionPeriod)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

TServiceDiscoveryEndpointsConfig::TServiceDiscoveryEndpointsConfig()
{
    RegisterParameter("cluster", Cluster);
    RegisterParameter("endpoint_set_id", EndpointSetId);
    RegisterParameter("update_period", UpdatePeriod)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

TBalancingChannelConfig::TBalancingChannelConfig()
{
    RegisterParameter("addresses", Addresses)
        .Optional();
    RegisterParameter("endpoints", Endpoints)
        .Optional();

    RegisterPostprocessor([&] {
        int endpointConfigCount = 0;
        if (Addresses) {
            ++endpointConfigCount;
        }
        if (Endpoints) {
            ++endpointConfigCount;
        }
        if (endpointConfigCount != 1) {
            THROW_ERROR_EXCEPTION("Exactly one of \"addresses\" and \"endpoints\" must be specified");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TThrottlingChannelConfig::TThrottlingChannelConfig()
{
    RegisterParameter("rate_limit", RateLimit)
        .GreaterThan(0)
        .Default(10);
}

////////////////////////////////////////////////////////////////////////////////

TThrottlingChannelDynamicConfig::TThrottlingChannelDynamicConfig()
{
    RegisterParameter("rate_limit", RateLimit)
        .GreaterThan(0)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TResponseKeeperConfig::TResponseKeeperConfig()
{
    RegisterParameter("expiration_time", ExpirationTime)
        .Default(TDuration::Minutes(5));
    RegisterParameter("max_eviction_busy_time", MaxEvictionTickTime)
        .Default(TDuration::MilliSeconds(10));
    RegisterParameter("enable_warmup", EnableWarmup)
        .Default(true);
    RegisterParameter("warmup_time", WarmupTime)
        .Default(TDuration::Minutes(6));
    RegisterPostprocessor([&] () {
        if (EnableWarmup && WarmupTime < ExpirationTime) {
            THROW_ERROR_EXCEPTION("\"warmup_time\" cannot be less than \"expiration_time\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TMultiplexingBandConfig::TMultiplexingBandConfig()
{
    RegisterParameter("tos_level", TosLevel)
        .Default(NYT::NBus::DefaultTosLevel);

    RegisterParameter("network_to_tos_level", NetworkToTosLevel)
        .Default();
}

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
    mergedConfig->Postprocess();
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
