#pragma once

#include "public.h"

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/misc/enum.h>

#include <vector>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERequestTracingMode,
    (Enable)
    (Disable)
    (Force)
);

////////////////////////////////////////////////////////////////////////////////

class THistogramExponentialBounds
    : public NYTree::TYsonSerializable
{
public:
    TDuration Min;
    TDuration Max;

    THistogramExponentialBounds();
};

DEFINE_REFCOUNTED_TYPE(THistogramExponentialBounds)

////////////////////////////////////////////////////////////////////////////////

class THistogramConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<THistogramExponentialBoundsPtr> ExponentialBounds;
    std::optional<std::vector<TDuration>> CustomBounds;

    THistogramConfig();
};

DEFINE_REFCOUNTED_TYPE(THistogramConfig)

////////////////////////////////////////////////////////////////////////////////

// Common options shared between all services in one server.
class TServiceCommonConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnablePerUserProfiling;
    THistogramConfigPtr HistogramTimerProfiling;
    ERequestTracingMode TracingMode;

    TServiceCommonConfig();
};

DEFINE_REFCOUNTED_TYPE(TServiceCommonConfig)

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public TServiceCommonConfig
{
public:
    THashMap<TString, NYTree::INodePtr> Services;

    TServerConfig();
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TServiceConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<bool> EnablePerUserProfiling;
    std::optional<ERequestTracingMode> TracingMode;
    THistogramConfigPtr HistogramTimerProfiling;
    THashMap<TString, TMethodConfigPtr> Methods;
    std::optional<int> AuthenticationQueueSizeLimit;
    std::optional<TDuration> PendingPayloadsTimeout;

    TServiceConfig();
};

DEFINE_REFCOUNTED_TYPE(TServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TMethodConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<bool> Heavy;
    std::optional<int> QueueSizeLimit;
    std::optional<int> ConcurrencyLimit;
    std::optional<NLogging::ELogLevel> LogLevel;
    std::optional<TDuration> LoggingSuppressionTimeout;
    NConcurrency::TThroughputThrottlerConfigPtr RequestBytesThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr LoggingSuppressionFailedRequestThrottler;
    std::optional<ERequestTracingMode> TracingMode;

    TMethodConfig();
};

DEFINE_REFCOUNTED_TYPE(TMethodConfig)

////////////////////////////////////////////////////////////////////////////////

class TRetryingChannelConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Time to wait between consequent attempts.
    TDuration RetryBackoffTime;

    //! Maximum number of retry attempts to make.
    int RetryAttempts;

    //! Maximum time to spend while retrying.
    //! If null then no limit is enforced.
    std::optional<TDuration> RetryTimeout;

    TRetryingChannelConfig();
};

DEFINE_REFCOUNTED_TYPE(TRetryingChannelConfig)

////////////////////////////////////////////////////////////////////////////////

class TBalancingChannelConfigBase
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Timeout for |Discover| requests.
    TDuration DiscoverTimeout;

    //! Timeout for acknowledgement of all RPC requests going through the channel.
    TDuration AcknowledgementTimeout;

    //! Interval between automatic rediscovery of active peers.
    /*!
     *  Discovery is started automatically if no active peers are known.
     *  In some cases, however, this is not enough.
     *  E.g. a follower may become active and thus eligible for load balancing.
     *  This setting controls the period of time after which the channel
     *  starts rediscovering peers even if an active one is known.
     */
    TDuration RediscoverPeriod;

    //! A random duration from 0 to #RediscoverSplay is added to #RediscoverPeriod on each
    //! rediscovery attempt.
    TDuration RediscoverSplay;

    //! Time between consequent attempts to reconnect to a peer, which
    //! returns a hard failure (i.e. non-OK response) to |Discover| request.
    TDuration HardBackoffTime;

    //! Time between consequent attempts to reconnect to a peer, which
    //! returns a soft failure (i.e. "down" response) to |Discover| request.
    TDuration SoftBackoffTime;

    TBalancingChannelConfigBase();
};

////////////////////////////////////////////////////////////////////////////////

class TDynamicChannelPoolConfig
    : public TBalancingChannelConfigBase
{
public:
    //! Maximum number of peers to query in parallel when locating alive ones.
    int MaxConcurrentDiscoverRequests;

    //! For sticky mode: number of consistent hash tokens to assign to each peer.
    int HashesPerPeer;

    //! In case too many peers are known, the pool will only maintain this many peers.
    int MaxPeerCount;

    //! When more than #MaxPeerCount peers are known an attempt to add more is
    //! typically ignored. To avoid stucking with the same peer set forever, one
    //! random peer could be evicted after #RandomPeerEvictionPeriod.
    TDuration RandomPeerEvictionPeriod;

    TDynamicChannelPoolConfig();
};

DEFINE_REFCOUNTED_TYPE(TDynamicChannelPoolConfig)

////////////////////////////////////////////////////////////////////////////////

class TServiceDiscoveryEndpointsConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Cluster;
    TString EndpointSetId;
    TDuration UpdatePeriod;

    TServiceDiscoveryEndpointsConfig();
};

DEFINE_REFCOUNTED_TYPE(TServiceDiscoveryEndpointsConfig)

////////////////////////////////////////////////////////////////////////////////

class TBalancingChannelConfig
    : public TDynamicChannelPoolConfig
{
public:
    //! First option: static list of addresses.
    std::optional<std::vector<TString>> Addresses;

    //! Second option: SD endpoints.
    TServiceDiscoveryEndpointsConfigPtr Endpoints;

    TBalancingChannelConfig();
};

DEFINE_REFCOUNTED_TYPE(TBalancingChannelConfig)

////////////////////////////////////////////////////////////////////////////////

class TThrottlingChannelConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Maximum allowed number of requests per second.
    int RateLimit;

    TThrottlingChannelConfig();
};

DEFINE_REFCOUNTED_TYPE(TThrottlingChannelConfig)

////////////////////////////////////////////////////////////////////////////////

class TThrottlingChannelDynamicConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    std::optional<int> RateLimit;

    TThrottlingChannelDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TThrottlingChannelDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TResponseKeeperConfig
    : public NYTree::TYsonSerializable
{
public:
    //! For how long responses are kept in memory.
    TDuration ExpirationTime;

    //! Maximum time an eviction tick can spend.
    TDuration MaxEvictionTickTime;

    //! If |true| then initial warmup is enabled. In particular, #WarmupTime and #ExpirationTime are
    //! checked against each other. If |false| then initial warmup is disabled and #WarmupTime is ignored.
    bool EnableWarmup;

    //! For how long the keeper remains passive after start and merely collects all responses.
    TDuration WarmupTime;

    TResponseKeeperConfig();
};

DEFINE_REFCOUNTED_TYPE(TResponseKeeperConfig)

////////////////////////////////////////////////////////////////////////////////

class TMultiplexingBandConfig
    : public NYTree::TYsonSerializable
{
public:
    int TosLevel;
    THashMap<TString, int> NetworkToTosLevel;

    TMultiplexingBandConfig();
};

DEFINE_REFCOUNTED_TYPE(TMultiplexingBandConfig)

////////////////////////////////////////////////////////////////////////////////

class TDispatcherConfig
    : public NYTree::TYsonSerializable
{
public:
    static constexpr int DefaultHeavyPoolSize = 16;
    static constexpr int DefaultCompressionPoolSize = 8;
    int HeavyPoolSize;
    int CompressionPoolSize;

    TEnumIndexedVector<EMultiplexingBand, TMultiplexingBandConfigPtr> MultiplexingBands;

    TDispatcherConfig();
    TDispatcherConfigPtr ApplyDynamic(const TDispatcherDynamicConfigPtr& dynamicConfig) const;
};

DEFINE_REFCOUNTED_TYPE(TDispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TDispatcherDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<int> HeavyPoolSize;
    std::optional<int> CompressionPoolSize;

    std::optional<TEnumIndexedVector<EMultiplexingBand, TMultiplexingBandConfigPtr>> MultiplexingBands;

    TDispatcherDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TDispatcherDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
