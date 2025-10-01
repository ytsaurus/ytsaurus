#pragma once

#include "private.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/library/query/base/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

NYT::NQueryClient::TConstExpressionPtr ConvertToConstExpression(
    const NTableClient::TTableSchemaPtr& schema,
    DB::QueryTreeNodePtr node);

std::vector<NChunkClient::TReadRange> InferReadRange(
    DB::QueryTreeNodePtr filterNode,
    const NTableClient::TTableSchemaPtr& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
