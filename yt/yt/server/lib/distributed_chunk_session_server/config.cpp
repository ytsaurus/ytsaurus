#include "config.h"

namespace NYT::NDistributedChunkSessionServer {

////////////////////////////////////////////////////////////////////////////////

void TDistributedChunkSessionServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("session_timeout", &TThis::SessionTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("data_node_rpc_timeout", &TThis::DataNodeRpcTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
