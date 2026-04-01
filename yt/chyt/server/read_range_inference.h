#pragma once

#include "private.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/library/query/base/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

NYT::NQueryClient::TConstExpressionPtr ConvertToConstExpression(
    DB::QueryTreeNodePtr node,
    const NTableClient::TTableSchemaPtr& schema,
    const TCompositeSettingsPtr& settings,
    bool transformNullIn = false);

std::vector<NChunkClient::TReadRange> InferReadRange(
    DB::QueryTreeNodePtr filterNode,
    const NTableClient::TTableSchemaPtr& schema,
    const DB::Settings& settings);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
