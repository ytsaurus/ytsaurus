#pragma once

#include <yt/yt/library/query/engine_api/column_evaluator.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

// Returns IColumnEvaluatorCache that wraps the
// standard column evaluator cache with an additional TTableSchemaPtr-keyed cache
// to avoid redundant expression parsing for repeated schemas.
NQueryClient::IColumnEvaluatorCachePtr CreateFastColumnEvaluatorCache();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
