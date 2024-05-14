#pragma once

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr InferInputSchema(
    const std::vector<TTableSchemaPtr>& schemas,
    bool discardKeyColumns);

////////////////////////////////////////////////////////////////////////////////

void ValidateFullSyncIndexSchema(
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema);

const TColumnSchema& FindUnfoldingColumnAndValidate(
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema);

void ValidateColumnsAreInIndexLockGroup(
    const NQueryClient::TColumnSet& columns,
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
