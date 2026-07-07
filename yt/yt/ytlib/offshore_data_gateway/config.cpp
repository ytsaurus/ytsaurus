#include "config.h"

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

void TOffshoreDataGatewayChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("data_gateway_update_period", &TThis::DataGatewayUpdatePeriod)
        .Default(std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
