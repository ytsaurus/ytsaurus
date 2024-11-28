#pragma once

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void ValidateFullSyncIndexSchema(
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema);

void ValidateUnfoldingIndexSchema(
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema,
    const TString& unfoldedColumnName);

const TColumnSchema& FindUnfoldedColumnAndValidate(
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema);

void ValidateColumnsAreInIndexLockGroup(
    const TColumnSet& columns,
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema);

void ValidateUniqueIndexSchema(
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema);

bool IsValidUnfoldedColumnPair(
    const NTableClient::TLogicalTypePtr& tableColumnType,
    const NTableClient::TLogicalTypePtr& indexColumnType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
