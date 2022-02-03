#include "config.h"

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

TDialerConfig::TDialerConfig()
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

////////////////////////////////////////////////////////////////////////////////

TAddressResolverConfig::TAddressResolverConfig()
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
    RegisterParameter("retry_delay", RetryDelay)
        .Default(TDuration::MilliSeconds(200));
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
