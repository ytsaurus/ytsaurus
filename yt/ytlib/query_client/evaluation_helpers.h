#pragma once

#include "public.h"
#include "callbacks.h"
#include "cg_types.h"
#include "plan_fragment.h"


namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

bool CountRow(i64* limit);

TJoinEvaluator GetJoinEvaluator(
    const TJoinClause& joinClause,
    const TConstExpressionPtr& predicate,
    TExecuteQuery executeCallback);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

