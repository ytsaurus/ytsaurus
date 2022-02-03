#include "config.h"

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

TRemoteTimestampProviderConfig::TRemoteTimestampProviderConfig()
{
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(3));
    RegisterParameter("latest_timestamp_update_period", LatestTimestampUpdatePeriod)
        // COMPAT(babenko)
        .Alias("update_period")
        .Default(TDuration::MilliSeconds(500));

    RegisterParameter("batch_period", BatchPeriod)
        .Default(TDuration::MilliSeconds(10));

    RegisterParameter("enable_timestamp_provider_discovery", EnableTimestampProviderDiscovery)
        .Default(false);
    RegisterParameter("timestamp_provider_discovery_period", TimestampProviderDiscoveryPeriod)
        .Default(TDuration::Minutes(1));
    RegisterParameter("timestamp_provider_discovery_period_splay", TimestampProviderDiscoveryPeriodSplay)
        .Default(TDuration::Seconds(10));

    RegisterPreprocessor([&] {
        RetryAttempts = 100;
        RetryTimeout = TDuration::Minutes(3);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
