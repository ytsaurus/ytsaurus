#pragma once

#include <yql/essentials/ast/yql_expr.h>


namespace NYql::NYtflow::NPrivate {

TExprNode::TPtr SelectExtendImplementation(
    const TExprNode::TPtr& operation,
    bool hasNonDeterministicFunctions,
    TExprContext& ctx);

} // namespace NYql::NYtflow::NPrivate
