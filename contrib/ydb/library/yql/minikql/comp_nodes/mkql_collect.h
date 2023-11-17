#pragma once
#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapCollect(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
