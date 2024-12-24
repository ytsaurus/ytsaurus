#include "config.h"

#include <yt/yt/client/chunk_client/public.h>

namespace NYT::NDistributedChunkSessionClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

void TDistributedChunkSessionControllerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("data_node_ping_period", &TThis::DataNodePingPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("write_session_ping_period", &TThis::WriteSessionPingPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("account", &TThis::Account);

    registrar.Parameter("replication_factor", &TThis::ReplicationFactor)
        .Default(DefaultReplicationFactor);

    registrar.Parameter("node_rpc_timeout", &TThis::NodeRpcTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

void TDistributedChunkWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(300));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
