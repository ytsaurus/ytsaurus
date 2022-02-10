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

void TAddressResolverConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_ipv4", &TThis::EnableIPv4)
        .Default(false);
    registrar.Parameter("enable_ipv6", &TThis::EnableIPv6)
        .Default(true);
    registrar.Parameter("localhost_name_override", &TThis::LocalHostNameOverride)
        .Alias("localhost_fqdn")
        .Default();
    registrar.Parameter("resolve_hostname_into_fqdn", &TThis::ResolveHostNameIntoFqdn)
        .Default(true);
    registrar.Parameter("retries", &TThis::Retries)
        .Default(25);
    registrar.Parameter("retry_delay", &TThis::RetryDelay)
        .Default(TDuration::MilliSeconds(200));
    registrar.Parameter("resolve_timeout", &TThis::ResolveTimeout)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("max_resolve_timeout", &TThis::MaxResolveTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("warning_timeout", &TThis::WarningTimeout)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("jitter", &TThis::Jitter)
        .Default(0.5);

    registrar.Preprocessor([] (TThis* config) {
        config->RefreshTime = TDuration::Seconds(60);
        config->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(120);
        config->ExpireAfterFailedUpdateTime = TDuration::Seconds(30);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
