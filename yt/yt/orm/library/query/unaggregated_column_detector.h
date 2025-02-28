#pragma once

#include <yt/yt/library/query/base/ast.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

// Checks if expression contains a column reference without aggregation function applied.
bool HasUnaggregatedColumn(NQueryClient::NAst::TExpressionPtr expression);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
