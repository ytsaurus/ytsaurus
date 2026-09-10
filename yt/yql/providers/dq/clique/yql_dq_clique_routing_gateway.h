#pragma once

#include <contrib/ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>

#include <yt/yql/providers/dq/common/yql_dq_clique.h>

namespace NYql {

TIntrusivePtr<IDqGateway> CreateDqCliqueRoutingGateway(
    TIntrusivePtr<IDqGateway> defaultGateway,
    TDqYtClusterResolver resolveYtCluster);

} // namespace NYql
