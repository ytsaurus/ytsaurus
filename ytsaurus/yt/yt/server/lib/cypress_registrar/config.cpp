#include "config.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TCypressRegistrarConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("update_period", &TThis::UpdatePeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("root_node_ttl", &TThis::RootNodeTtl)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("alive_child_ttl", &TThis::AliveChildTtl)
        .Default();

    registrar.Parameter("request_timeout", &TThis::RequestTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
