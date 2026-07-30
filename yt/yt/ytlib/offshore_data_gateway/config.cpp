#include "config.h"

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

void TOffshoreDataGatewayChannelTestingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("bypass_cache", &TThis::BypassCache)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TOffshoreDataGatewayChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("data_gateway_update_period", &TThis::DataGatewayUpdatePeriod)
        .Default();

    registrar.Parameter("testing", &TThis::Testing)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
