#pragma once

#include "public.h"

#include <core/compression/codec.h>

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public TYsonSerializable
{
public:
    yhash_map<Stroka, NYTree::INodePtr> Services;

    TServerConfig()
    {
        RegisterParameter("services", Services)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TServiceConfig
    : public TYsonSerializable
{
public:
    yhash_map<Stroka, TMethodConfigPtr> Methods;

    TServiceConfig()
    {
        RegisterParameter("methods", Methods)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TServiceConfig)

////////////////////////////////////////////////////////////////////////////////

class TMethodConfig
    : public TYsonSerializable
{
public:
    TNullable<bool> RequestHeavy;
    TNullable<bool> ResponseHeavy;
    TNullable<NCompression::ECodec> ResponseCodec;
    TNullable<int> MaxQueueSize;

    TMethodConfig()
    {
        RegisterParameter("request_heavy", RequestHeavy)
            .Default();
        RegisterParameter("response_heavy", ResponseHeavy)
            .Default();
        RegisterParameter("response_codec", ResponseCodec)
            .Default();
        RegisterParameter("max_queue_size", MaxQueueSize)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TMethodConfig)

////////////////////////////////////////////////////////////////////////////////

class TRetryingChannelConfig
    : public virtual TYsonSerializable
{
public:
    //! Time to wait between consequent attempts.
    TDuration RetryBackoffTime;

    //! Maximum number of retry attempts to make.
    int RetryAttempts;

    //! Maximum time to spend while retrying.
    //! If |Null| then no limit is enforced.
    TNullable<TDuration> RetryTimeout;

    TRetryingChannelConfig()
    {
        RegisterParameter("retry_backoff_time", RetryBackoffTime)
            .Default(TDuration::Seconds(3));
        RegisterParameter("retry_attempts", RetryAttempts)
            .GreaterThanOrEqual(1)
            .Default(10);
        RegisterParameter("retry_timeout", RetryTimeout)
            .GreaterThanOrEqual(TDuration::Zero())
            .Default(Null);
    }
};

DEFINE_REFCOUNTED_TYPE(TRetryingChannelConfig)

////////////////////////////////////////////////////////////////////////////////

class TBalancingChannelConfig
    : public virtual TYsonSerializable
{
public:
    //! List of seed addresses.
    std::vector<Stroka> Addresses;

    //! Timeout for Discovery requests.
    TDuration DiscoverTimout;

    //! Time between consequent attempts to reconnect to a peer, which
    //! returns a hard failure (i.e. non-OK response) to Discover request.
    TDuration HardBackoffTime;

    //! Time between consequent attempts to reconnect to a peer, which
    //! returns a soft failure (i.e. "down" response) to Discover request.
    TDuration SoftBackoffTime;

    TBalancingChannelConfig()
    {
        RegisterParameter("addresses", Addresses);
        RegisterParameter("discover_timeout", DiscoverTimout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("hard_backoff_time", HardBackoffTime)
            .Default(TDuration::Seconds(15));
        RegisterParameter("soft_backoff_time", SoftBackoffTime)
            .Default(TDuration::Seconds(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TBalancingChannelConfig)

////////////////////////////////////////////////////////////////////////////////

class TThrottlingChannelConfig
    : public TYsonSerializable
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
    : public TYsonSerializable
{
public:
    //! For how long responses are kept in memory.
    TDuration ExpirationTime;

    TResponseKeeperConfig()
    {
        RegisterParameter("expiration_time", ExpirationTime)
            .Default(TDuration::Minutes(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TResponseKeeperConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
