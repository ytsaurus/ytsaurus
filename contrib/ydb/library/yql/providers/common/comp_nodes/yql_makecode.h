#pragma once
#include <contrib/ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <contrib/ydb/library/yql/ast/yql_expr.h>

namespace NKikimr {
namespace NMiniKQL {

template <NYql::TExprNode::EType Type>
IComputationNode* WrapMakeCode(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);

}
}
