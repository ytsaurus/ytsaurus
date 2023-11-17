#pragma once

#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <contrib/ydb/library/yql/minikql/mkql_stats_registry.h>
#include <contrib/ydb/library/yql/dq/integration/transform/yql_dq_task_transform.h>

namespace NYql {
namespace NTaskRunnerProxy {

int CreateTaskCommandExecutor(NKikimr::NMiniKQL::TComputationNodeFactory compFactory, TTaskTransformFactory taskTransformFactory, NKikimr::NMiniKQL::IStatsRegistry* jobStats, bool terminateOnError = false);

} // namespace NTaskRunnerProxy
} // namespace NYql
