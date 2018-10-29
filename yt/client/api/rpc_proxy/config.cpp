#include "config.h"

namespace NYT {
namespace NApi {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

TConnectionConfig::TConnectionConfig()
{
    RegisterParameter("cluster_url", ClusterUrl)
        .Default();
    RegisterParameter("proxy_role", ProxyRole)
        .Optional();
    RegisterParameter("addresses", Addresses)
        .Default();
    RegisterParameter("channel_pool_size", ChannelPoolSize)
        .Default(3);
    RegisterParameter("channel_pool_rebalance_interval", ChannelPoolRebalanceInterval)
        .Default(TDuration::Minutes(1));

    RegisterParameter("ping_period", PingPeriod)
        .Default(TDuration::Seconds(3));

    RegisterParameter("proxy_list_update_period", ProxyListUpdatePeriod)
        .Default(TDuration::Minutes(5));
    RegisterParameter("proxy_list_retry_period", ProxyListRetryPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("max_proxy_list_retry_period", MaxProxyListRetryPeriod)
        .Default(TDuration::Seconds(30));
    RegisterParameter("max_proxy_list_update_attempts", MaxProxyListUpdateAttempts)
        .Default(3);

    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(30));
    RegisterParameter("timestamp_provider_update_period", TimestampProviderUpdatePeriod)
        .Default(TDuration::Seconds(3));
    RegisterParameter("default_transaction_timeout", DefaultTransactionTimeout)
        .Default(TDuration::Seconds(15));
    RegisterParameter("default_select_rows_timeout", DefaultSelectRowsTimeout)
        .Default(TDuration::Seconds(30));
    RegisterParameter("default_ping_period", DefaultPingPeriod)
        .Default(TDuration::Seconds(5));
    RegisterParameter("bus_client", BusClient)
        .DefaultNew();
    RegisterParameter("http_client", HttpClient)
        .DefaultNew();

    RegisterParameter("enable_proxy_discovery", EnableProxyDiscovery)
        .Default(true);

    RegisterPostprocessor([this] {
        if (!ClusterUrl && Addresses.empty()) {
            THROW_ERROR_EXCEPTION("Either \"cluster_url\" or \"addresses\" must be specified");
        }

        if (!EnableProxyDiscovery) {
            if (Addresses.empty()) {
                THROW_ERROR_EXCEPTION("\"addresses\" must be specified");
            }

            ClusterUrl.Reset();
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT
