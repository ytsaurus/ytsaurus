#pragma once

#include "public.h"

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void ValidateIndexSchema(
    NTabletClient::ESecondaryIndexKind kind,
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema,
    const std::optional<std::string>& predicate,
    const TTableSchemaPtr& evaluatedColumnsSchema,
    const std::optional<NTabletClient::TUnfoldedColumns>& unfoldedColumns = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

void ValidateColumnsCollision(const TTableSchema& lhs, const TTableSchema& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
