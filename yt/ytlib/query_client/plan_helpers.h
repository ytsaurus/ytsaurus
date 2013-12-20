#pragma once

#include "public.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

const TDataSplit& GetHeaviestSplit(const TOperator* op);

TTableSchema InferTableSchema(const TOperator* op);

TKeyColumns InferKeyColumns(const TOperator* op);

EValueType InferType(const TExpression* expr, bool ignoreCached = false);

Stroka InferName(const TExpression* expr, bool ignoreCached = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

