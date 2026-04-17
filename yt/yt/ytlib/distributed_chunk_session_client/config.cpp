#include "config.h"

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

void TDistributedChunkSessionControllerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("session_ping_period", &TThis::SessionPingPeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("session_timeout", &TThis::SessionTimeout)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("create_chunk_timeout", &TThis::CreateChunkTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("account", &TThis::Account);

    registrar.Parameter("medium", &TThis::MediumName)
        .Default("default");

    registrar.Parameter("is_vital", &TThis::IsVital)
        .Default(true);

    registrar.Parameter("node_rpc_timeout", &TThis::NodeRpcTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("max_consecutive_ping_failures", &TThis::MaxConsecutivePingFailures)
        .Default(5);
}

////////////////////////////////////////////////////////////////////////////////

void TDistributedChunkWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
