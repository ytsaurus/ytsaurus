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

void TDistributedChunkSessionPoolConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_active_sessions_per_slot", &TThis::MaxActiveSessionsPerSlot)
        .Default(3)
        .GreaterThan(0);

    registrar.Parameter("chunk_seal_retry_backoff", &TThis::ChunkSealRetryBackoff)
        .Default(TExponentialBackoffOptions{
            .InvocationCount = std::numeric_limits<int>::max(),
            .MinBackoff = TDuration::MilliSeconds(100),
            .MaxBackoff = TDuration::Seconds(5),
        });

    registrar.Parameter("chunk_seal_rpc_timeout", &TThis::ChunkSealRpcTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

void TDistributedChunkWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
