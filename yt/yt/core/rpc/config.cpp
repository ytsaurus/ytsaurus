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
    RegisterParameter("code_counting", EnableErrorCodeCounting)
        .Default(false);
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
    RegisterParameter("code_counting", EnableErrorCodeCounting)
        .Optional();
    RegisterParameter("histogram_timer_profiling", HistogramTimerProfiling)
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

void TRetryingChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("retry_backoff_time", &TThis::RetryBackoffTime)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("retry_attempts", &TThis::RetryAttempts)
        .GreaterThanOrEqual(1)
        .Default(10);
    registrar.Parameter("retry_timeout", &TThis::RetryTimeout)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TBalancingChannelConfigBase::Register(TRegistrar registrar)
{
    registrar.Parameter("discover_timeout", &TThis::DiscoverTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("acknowledgement_timeout", &TThis::AcknowledgementTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("rediscover_period", &TThis::RediscoverPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("rediscover_splay", &TThis::RediscoverSplay)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("hard_backoff_time", &TThis::HardBackoffTime)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("soft_backoff_time", &TThis::SoftBackoffTime)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicChannelPoolConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_concurrent_discover_requests", &TThis::MaxConcurrentDiscoverRequests)
        .GreaterThan(0)
        .Default(10);
    registrar.Parameter("hashes_per_peer", &TThis::HashesPerPeer)
        .GreaterThan(0)
        .Default(10);
    registrar.Parameter("max_peer_count", &TThis::MaxPeerCount)
        .GreaterThan(1)
        .Default(100);
    registrar.Parameter("random_peer_eviction_period", &TThis::RandomPeerEvictionPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("enable_peer_polling", &TThis::EnablePeerPolling)
        .Default(false);
    registrar.Parameter("peer_polling_period", &TThis::PeerPollingPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("peer_polling_period_splay", &TThis::PeerPollingPeriodSplay)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("peer_polling_request_timeout", &TThis::PeerPollingRequestTimeout)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

TServiceDiscoveryEndpointsConfig::TServiceDiscoveryEndpointsConfig()
{
    RegisterParameter("cluster", Cluster)
        .Default();
    RegisterParameter("clusters", Clusters)
        .Default();
    RegisterParameter("endpoint_set_id", EndpointSetId);
    RegisterParameter("update_period", UpdatePeriod)
        .Default(TDuration::Seconds(60));

    RegisterPostprocessor([&] {
        YT_VERIFY(Cluster.has_value() ^ !Clusters.empty());

        if (Clusters.empty()) {
            Clusters.push_back(*Cluster);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TBalancingChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("addresses", &TThis::Addresses)
        .Optional();
    registrar.Parameter("endpoints", &TThis::Endpoints)
        .Optional();
    registrar.Parameter("hedging_delay", &TThis::HedgingDelay)
        .Optional();
    registrar.Parameter("cancel_primary_request_on_hedging", &TThis::CancelPrimaryRequestOnHedging)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        int endpointConfigCount = 0;
        if (config->Addresses) {
            ++endpointConfigCount;
        }
        if (config->Endpoints) {
            ++endpointConfigCount;
        }
        if (endpointConfigCount != 1) {
            THROW_ERROR_EXCEPTION("Exactly one of \"addresses\" and \"endpoints\" must be specified");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TThrottlingChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rate_limit", &TThis::RateLimit)
        .GreaterThan(0)
        .Default(10);
}

////////////////////////////////////////////////////////////////////////////////

void TThrottlingChannelDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rate_limit", &TThis::RateLimit)
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
