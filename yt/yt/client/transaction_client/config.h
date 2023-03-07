#pragma once

#include "public.h"

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TRemoteTimestampProviderConfig
    : public NRpc::TBalancingChannelConfig
    , public NRpc::TRetryingChannelConfig
{
public:
    //! Timeout for RPC requests to timestamp provider.
    TDuration RpcTimeout;

    //! Interval between consecutive updates of latest timestamp.
    TDuration LatestTimestampUpdatePeriod;

    TRemoteTimestampProviderConfig()
    {
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(3));
        RegisterParameter("latest_timestamp_update_period", LatestTimestampUpdatePeriod)
            // COMPAT(babenko)
            .Alias("update_period")
            .Default(TDuration::Seconds(3));
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteTimestampProviderConfig)

////////////////////////////////////////////////////////////////////////////////

class TBatchingRemoteTimestampProviderConfig
    : public TRemoteTimestampProviderConfig
{
public:
    //! All generation requests coming within this period are batched
    //! together.
    TDuration BatchPeriod;

    TBatchingRemoteTimestampProviderConfig()
    {
        RegisterParameter("batch_period", BatchPeriod)
            .Default(TDuration::MilliSeconds(10));
    }
};

DEFINE_REFCOUNTED_TYPE(TBatchingRemoteTimestampProviderConfig)

////////////////////////////////////////////////////////////////////////////////

class TRemoteTimestampProviderWithDiscoveryConfig
    : public TRemoteTimestampProviderConfig
{
public:
    bool EnableTimestampProviderDiscovery;
    TDuration TimestampProviderDiscoveryPeriod;

    TRemoteTimestampProviderWithDiscoveryConfig()
    {
        RegisterParameter("enable_timestamp_provider_discovery", EnableTimestampProviderDiscovery)
            .Default(false);
        RegisterParameter("timestamp_provider_discovery_period", TimestampProviderDiscoveryPeriod)
            .Default(TDuration::Minutes(1));
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteTimestampProviderWithDiscoveryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
