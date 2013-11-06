#pragma once

#include "public.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

const TDataSplit& GetHeaviestSplit(const TOperator* op);

TTableSchema InferTableSchema(const TOperator* op);

TKeyColumns InferKeyColumns(const TOperator* op);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

