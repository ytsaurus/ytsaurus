#pragma once

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void ValidateIndexSchema(
    NTabletClient::ESecondaryIndexKind kind,
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema,
    const std::optional<TString>& predicate,
    const TTableSchemaPtr& evaluatedColumnsSchema,
    const std::optional<TString> unfoldedColumnName = std::nullopt);

const TColumnSchema& FindUnfoldingColumnAndValidate(
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema,
    const std::optional<TString>& predicate,
    const TTableSchemaPtr& evaluatedColumnsSchema);

////////////////////////////////////////////////////////////////////////////////

void ValidateNoNameCollisions(const TTableSchema& lhs, const TTableSchema& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
