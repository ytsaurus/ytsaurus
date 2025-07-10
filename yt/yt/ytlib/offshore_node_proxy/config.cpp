#include "config.h"

namespace NYT::NOffshoreNodeProxy {

////////////////////////////////////////////////////////////////////////////////

void TOffshoreNodeProxyChannelConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
