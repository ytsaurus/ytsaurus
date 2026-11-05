#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_node.h>


namespace NYql {

class IYtflowLookupProviderRegistry;

} // namespace NYql

namespace NKikimr::NMiniKQL {

IComputationNode* WrapYtflowLookupJoin(
    TCallable& callable,
    const TComputationNodeFactoryContext& ctx,
    const NYql::IYtflowLookupProviderRegistry& ytflowLookupProviderRegistry);

} // namespace NKikimr::NMiniKQL
