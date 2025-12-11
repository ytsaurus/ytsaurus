#include "config.h"

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

void TOffshoreDataGatewayChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
