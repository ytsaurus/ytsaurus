#pragma once
#include <contrib/ydb/library/yql/ast/yql_expr.h>

namespace NYql {

struct TTypeAnnotationContext;

TString CalculateLineage(const TExprNode& root, const TTypeAnnotationContext& ctx);

}
