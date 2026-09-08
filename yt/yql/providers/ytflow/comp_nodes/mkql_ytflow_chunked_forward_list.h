#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NKikimr::NMiniKQL {

IComputationNode* WrapYtflowChunkedForwardList(
    TCallable& callable,
    const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL
