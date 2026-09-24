#include "yql_dq_clique_routing_gateway.h"

namespace NYql {

TIntrusivePtr<IDqGateway> CreateDqCliqueRoutingGateway(
    TIntrusivePtr<IDqGateway> defaultGateway,
    TDqYtClusterResolver /*resolveYtCluster*/)
{
    return defaultGateway;
}

} // namespace NYql
