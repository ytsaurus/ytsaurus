#include "config.h"
#include "public.h"

#include <yt/yt/core/misc/backoff_strategy_config.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

void TNativeAuthenticationManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tvm_service", &TThis::TvmService)
        .Default();
    registrar.Parameter("enable_validation", &TThis::EnableValidation)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TNativeAuthenticationManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_validation", &TThis::EnableValidation)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TTvmBridgeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("self_tvm_id", &TThis::SelfTvmId)
        .Default(0);
    registrar.Parameter("refresh_period", &TThis::RefreshPeriod)
        .Default(TDuration::Hours(1));
    registrar.Parameter("ensure_tickets_backoff", &TThis::EnsureTicketsBackoff)
        .DefaultNew();
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(10));

    registrar.Preprocessor([] (TThis* config) {
        config->EnsureTicketsBackoff->MinBackoff = TDuration::Seconds(1);
        config->EnsureTicketsBackoff->MaxBackoff = TDuration::Seconds(1);
        config->EnsureTicketsBackoff->RetryCount = 10;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
