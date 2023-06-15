#include "config.h"

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

void TDnsOverRpcResolverConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("resolve_batching_period", &TThis::ResolveBatchingPeriod)
        .Default(TDuration::MilliSeconds(10));
    registrar.Parameter("resolve_rpc_timeout", &TThis::ResolveRpcTimeout)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns
