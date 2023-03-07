#pragma once

#include "public.h"

#include <yt/client/tablet_client/public.h>

namespace NYT::NTableClient {

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

//
// Validate that values from table with inputSchema also match outputSchema.
//
// allowSimpleTypeDeoptionalize:
// if set to true optional<T> will be compatible with T if T is simple type
//   this argument is dangerous and is used in some places for historical reasons.
TError ValidateTableSchemaCompatibility(
    const TTableSchema& inputSchema,
    const TTableSchema& outputSchema,
    bool ignoreSortOrder,
    bool allowSimpleTypeDeoptionalize = false);

//! Compared to #ValidateTableSchema, additionally validates
//! aggregated and computed columns (this involves calling some heavy QL-related
//! stuff which is missing in yt/client).
void ValidateTableSchemaHeavy(
    const TTableSchema& schema,
    bool isTableDynamic);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
