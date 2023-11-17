#pragma once
#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapChain1Map(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
