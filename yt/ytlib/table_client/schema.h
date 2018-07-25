#pragma once

#include "public.h"

#include <yt/client/tablet_client/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

void ValidateColumnSchemaUpdate(
    const TColumnSchema& oldColumn,
    const TColumnSchema& newColumn);

void ValidateTableSchemaUpdate(
    const TTableSchema& oldSchema,
    const TTableSchema& newSchema,
    bool isTableDynamic = false,
    bool isTableEmpty = false);

void ValidatePivotKey(
    const TOwningKey& pivotKey,
    const TTableSchema& schema);

TTableSchema InferInputSchema(
    const std::vector<TTableSchema>& schemas,
    bool discardKeyColumns);

TError ValidateTableSchemaCompatibility(
    const TTableSchema& inputSchema,
    const TTableSchema& outputSchema,
    bool ignoreSortOrder);

//! Compared to #ValidateTableSchema, additionally validates
//! aggregated and computed columns (this involves calling some heavy QL-related
//! stuff which is missing in yt/client).
void ValidateTableSchemaHeavy(
    const TTableSchema& schema,
    bool isTableDynamic);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
