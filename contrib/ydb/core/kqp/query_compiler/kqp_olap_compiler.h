#pragma once

#include <contrib/ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>

#include <contrib/ydb/core/kqp/provider/yql_kikimr_gateway.h>

namespace NKikimr {
namespace NKqp {

void CompileOlapProgram(const NYql::NNodes::TCoLambda& lambda, const NYql::TKikimrTableMetadata& tableMeta,
    NKqpProto::TKqpPhyOpReadOlapRanges& readProto, const std::vector<std::string>& resultColNames, NYql::TExprContext &exprCtx);

} // namespace NKqp
} // namespace NKikimr
