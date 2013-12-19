#pragma once

#include "public.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

const TDataSplit& GetHeaviestSplit(const TOperator* op);

TTableSchema InferTableSchema(const TOperator* op);

TKeyColumns InferKeyColumns(const TOperator* op);

EValueType InferType(const TExpression* expr, const TTableSchema& sourceSchema);

Stroka InferName(const TExpression* expr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

