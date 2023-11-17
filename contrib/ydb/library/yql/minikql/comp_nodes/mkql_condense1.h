#pragma once
#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapCondense1(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapSqueeze1(TCallable& callable, const TComputationNodeFactoryContext& ctx);

}
}
