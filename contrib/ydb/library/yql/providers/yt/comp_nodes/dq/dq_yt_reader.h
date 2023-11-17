#pragma once

#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <contrib/ydb/library/yql/minikql/mkql_stats_registry.h>
#include <contrib/ydb/library/yql/minikql/mkql_node.h>

namespace NYql::NDqs {

NKikimr::NMiniKQL::IComputationNode* WrapDqYtRead(NKikimr::NMiniKQL::TCallable& callable, NKikimr::NMiniKQL::IStatsRegistry* jobStats, const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ctx, bool useBlocks);

} // NYql
