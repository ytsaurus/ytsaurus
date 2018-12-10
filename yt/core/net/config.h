#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/misc/config.h>

namespace NYT {
namespace NNet {

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
    std::optional<TString> LocalHostFqdn;
    int Retries;
    TDuration ResolveTimeout;
    TDuration MaxResolveTimeout;
    TDuration WarningTimeout;

    TAddressResolverConfig()
    {
        RegisterParameter("enable_ipv4", EnableIPv4)
            .Default(false);
        RegisterParameter("enable_ipv6", EnableIPv6)
            .Default(true);
        RegisterParameter("localhost_fqdn", LocalHostFqdn)
            .Default();
        RegisterParameter("retries", Retries)
            .Default(25);
        RegisterParameter("resolve_timeout", ResolveTimeout)
            .Default(TDuration::Seconds(1));
        RegisterParameter("max_resolve_timeout", MaxResolveTimeout)
            .Default(TDuration::Seconds(15));
        RegisterParameter("warning_timeout", WarningTimeout)
            .Default(TDuration::Seconds(3));

        RegisterPreprocessor([this] () {
            RefreshTime = TDuration::Seconds(60);
            ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(120);
            ExpireAfterFailedUpdateTime = TDuration::Seconds(30);
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TAddressResolverConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT
