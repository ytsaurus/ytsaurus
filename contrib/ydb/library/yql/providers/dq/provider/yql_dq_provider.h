#pragma once

#include "yql_dq_gateway.h"

#include <contrib/ydb/library/yql/providers/common/metrics/metrics_registry.h>
#include <contrib/ydb/library/yql/core/yql_data_provider.h>
#include <contrib/ydb/library/yql/core/yql_udf_resolver.h>
#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <contrib/ydb/library/yql/core/file_storage/file_storage.h>

namespace NYql {

struct TDqState;
using TDqStatePtr = TIntrusivePtr<TDqState>;

using TExecTransformerFactory = std::function<IGraphTransformer*(const TDqStatePtr& state)>;

TDataProviderInitializer GetDqDataProviderInitializer(
    TExecTransformerFactory execTransformerFactory,
    const IDqGateway::TPtr& dqGateway,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
    const IMetricsRegistryPtr& metrics,
    const TFileStoragePtr& fileStorage,
    bool externalUser = false);

} // namespace NYql
