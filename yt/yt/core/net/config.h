#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

class TDialerConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableNoDelay;
    bool EnableAggressiveReconnect;

    TDuration MinRto;
    TDuration MaxRto;
    double RtoScale;

    TDialerConfig()
    {
        RegisterParameter("enable_no_delay", EnableNoDelay)
            .Default(true);
        RegisterParameter("enable_aggressive_reconnect", EnableAggressiveReconnect)
            .Default(false);
        RegisterParameter("min_rto", MinRto)
            .Default(TDuration::MilliSeconds(100));
        RegisterParameter("max_rto", MaxRto)
            .Default(TDuration::Seconds(30));
        RegisterParameter("rto_scale", RtoScale)
            .GreaterThan(0.0)
            .Default(2.0);

        RegisterPostprocessor([&] () {
            if (MaxRto < MinRto) {
                THROW_ERROR_EXCEPTION("\"max_rto\" should be greater than or equal to \"min_rto\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TDialerConfig)

////////////////////////////////////////////////////////////////////////////////

//! Configuration for TAddressResolver singleton.
class TAddressResolverConfig
    : public TAsyncExpiringCacheConfig
{
public:
    bool EnableIPv4;
    bool EnableIPv6;
    //! If true, when determining local host name, it will additionally be resolved
    //! into FQDN by calling |getaddrinfo|. Setting this option to false may be
    //! useful in MTN environment, in which hostnames are barely resolvable.
    //! NB: Set this option to false only if you are sure that process is not being
    //! exposed under localhost name to anyone; in particular, any kind of discovery
    //! should be done using some other kind of addresses.
    bool ResolveHostNameIntoFqdn;
    //! If set, localhost name will be forcefully set to the given value rather
    //! than retrieved via |NYT::NNet::UpdateLocalHostName|.
    std::optional<TString> LocalHostNameOverride;
    int Retries;
    TDuration ResolveTimeout;
    TDuration MaxResolveTimeout;
    double Jitter;
    TDuration WarningTimeout;

    TAddressResolverConfig()
    {
        RegisterParameter("enable_ipv4", EnableIPv4)
            .Default(false);
        RegisterParameter("enable_ipv6", EnableIPv6)
            .Default(true);
        RegisterParameter("localhost_name_override", LocalHostNameOverride)
            .Alias("localhost_fqdn")
            .Default();
        RegisterParameter("resolve_hostname_into_fqdn", ResolveHostNameIntoFqdn)
            .Default(true);
        RegisterParameter("retries", Retries)
            .Default(25);
        RegisterParameter("resolve_timeout", ResolveTimeout)
            .Default(TDuration::Seconds(1));
        RegisterParameter("max_resolve_timeout", MaxResolveTimeout)
            .Default(TDuration::Seconds(15));
        RegisterParameter("warning_timeout", WarningTimeout)
            .Default(TDuration::Seconds(3));
        RegisterParameter("jitter", Jitter)
            .Default(0.5);

        RegisterPreprocessor([this] () {
            RefreshTime = TDuration::Seconds(60);
            ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(120);
            ExpireAfterFailedUpdateTime = TDuration::Seconds(30);
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TAddressResolverConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
