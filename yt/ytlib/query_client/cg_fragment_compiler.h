#pragma once

#include "cg_types.h"

#include <core/codegen/module.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TCGQueryCallback CodegenEvaluate(
    const TConstQueryPtr& query,
    const TCGBinding& binding);

TCGExpressionCallback CodegenExpression(
    const TConstExpressionPtr& expression,
    const TTableSchema& tableSchema,
    const TCGBinding& binding);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

