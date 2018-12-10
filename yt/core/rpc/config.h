#pragma once

#include "public.h"

#include <yt/core/compression/public.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public NYTree::TYsonSerializable
{
public:
    THashMap<TString, NYTree::INodePtr> Services;

    TServerConfig()
    {
        RegisterParameter("services", Services)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TServiceConfig
    : public NYTree::TYsonSerializable
{
public:
    THashMap<TString, TMethodConfigPtr> Methods;

    static const int DefaultMaxAuthenticationQueueSize;
    int MaxAuthenticationQueueSize;

    TServiceConfig()
    {
        RegisterParameter("methods", Methods)
            .Default();
        RegisterParameter("max_authentication_queue_size", MaxAuthenticationQueueSize)
            .Default(DefaultMaxAuthenticationQueueSize);
    }
};

DEFINE_REFCOUNTED_TYPE(TServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TMethodConfig
    : public NYTree::TYsonSerializable
{
public:
    static const bool DefaultHeavy;
    bool Heavy;

    static const NCompression::ECodec DefaultResponseCodec;
    NCompression::ECodec ResponseCodec;

    static const int DefaultMaxQueueSize;
    int MaxQueueSize;

    static const int DefaultMaxConcurrency;
    int MaxConcurrency;

    static const NLogging::ELogLevel DefaultLogLevel;
    NLogging::ELogLevel LogLevel;

    TMethodConfig()
    {
        RegisterParameter("heavy", Heavy)
            .Default(DefaultHeavy);
        RegisterParameter("response_codec", ResponseCodec)
            .Default(DefaultResponseCodec);
        RegisterParameter("max_queue_size", MaxQueueSize)
            .Default(DefaultMaxQueueSize);
        RegisterParameter("max_concurrency", MaxConcurrency)
            .Default(DefaultMaxConcurrency);
        RegisterParameter("log_level", LogLevel)
            .Default(DefaultLogLevel);
    }
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

    TRetryingChannelConfig()
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
};

DEFINE_REFCOUNTED_TYPE(TRetryingChannelConfig)

////////////////////////////////////////////////////////////////////////////////

class TBalancingChannelConfigBase
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Timeout for |Discover| requests.
    TDuration DiscoverTimeout;

    //! Time between consequent attempts to reconnect to a peer, which
    //! returns a hard failure (i.e. non-OK response) to |Discover| request.
    TDuration HardBackoffTime;

    //! Time between consequent attempts to reconnect to a peer, which
    //! returns a soft failure (i.e. "down" response) to |Discover| request.
    TDuration SoftBackoffTime;

    TBalancingChannelConfigBase()
    {
        RegisterParameter("discover_timeout", DiscoverTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("hard_backoff_time", HardBackoffTime)
            .Default(TDuration::Seconds(15));
        RegisterParameter("soft_backoff_time", SoftBackoffTime)
            .Default(TDuration::Seconds(5));
    }
};

class TBalancingChannelConfig
    : public TBalancingChannelConfigBase
{
public:
    //! List of seed addresses.
    std::vector<TString> Addresses;

    //! Maximum number of peers to query in parallel when locating alive endpoint.
    int MaxConcurrentDiscoverRequests;

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

    //! For sticky mode: number of consistent hash tokens to assign to each peer.
    int HashesPerPeer;

    TBalancingChannelConfig()
    {
        RegisterParameter("addresses", Addresses);
        RegisterParameter("max_concurrent_discover_requests", MaxConcurrentDiscoverRequests)
            .GreaterThan(0)
            .Default(1);
        RegisterParameter("rediscover_period", RediscoverPeriod)
            .Default(TDuration::Minutes(5));
        RegisterParameter("rediscover_splay", RediscoverSplay)
            .Default(TDuration::Minutes(1));
        RegisterParameter("hashes_per_peer", HashesPerPeer)
            .Default(10);
    }
};

DEFINE_REFCOUNTED_TYPE(TBalancingChannelConfig)

////////////////////////////////////////////////////////////////////////////////

class TThrottlingChannelConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    //! Maximum allowed number of requests per second.
    int RateLimit;

    TThrottlingChannelConfig()
    {
        RegisterParameter("rate_limit", RateLimit)
            .GreaterThan(0)
            .Default(10);
    }
};

DEFINE_REFCOUNTED_TYPE(TThrottlingChannelConfig)

////////////////////////////////////////////////////////////////////////////////

class TResponseKeeperConfig
    : public NYTree::TYsonSerializable
{
public:
    //! For how long responses are kept in memory.
    TDuration ExpirationTime;

    //! If |true| then initial warmup is enabled. In particular, #WarmupTime and #ExpirationTime are
    //! checked against each other. If |false| then initial warmup is disabled and #WarmupTime is ignored.
    bool EnableWarmup;

    //! For how long the keeper remains passive after start and merely collects all responses.
    TDuration WarmupTime;

    TResponseKeeperConfig()
    {
        RegisterParameter("expiration_time", ExpirationTime)
            .Default(TDuration::Minutes(5));
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
};

DEFINE_REFCOUNTED_TYPE(TResponseKeeperConfig)

////////////////////////////////////////////////////////////////////////////////

class TMultiplexingBandConfig
    : public NYTree::TYsonSerializable
{
public:
    int TosLevel;

    TMultiplexingBandConfig()
    {
        RegisterParameter("tos_level", TosLevel)
            .Default(NYT::NBus::DefaultTosLevel);
    }
};

DEFINE_REFCOUNTED_TYPE(TMultiplexingBandConfig)

////////////////////////////////////////////////////////////////////////////////

class TDispatcherConfig
    : public NYTree::TYsonSerializable
{
public:
    static const int DefaultHeavyPoolSize;
    int HeavyPoolSize;

    TEnumIndexedVector<TMultiplexingBandConfigPtr, EMultiplexingBand> MultiplexingBands;

    TDispatcherConfig()
    {
        RegisterParameter("heavy_pool_size", HeavyPoolSize)
            .Default(DefaultHeavyPoolSize);
        RegisterParameter("multiplexing_bands", MultiplexingBands)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TDispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
