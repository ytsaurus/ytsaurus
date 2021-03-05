#pragma once

#include "public.h"

#include <yt/yt/client/tablet_client/public.h>

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
    const TUnversionedRow& pivotKey,
    const TTableSchema& schema,
    const TStringBuf& keyType = "pivot",
    bool validateRequired = false);

TTableSchemaPtr InferInputSchema(
    const std::vector<TTableSchemaPtr>& schemas,
    bool discardKeyColumns);

// Validates that values from table with inputSchema also match outputSchema.
//
// Result pair contains following elements:
//   1. Level of compatibility of the given schemas.
//   2. If schemas are fully compatible error is empty otherwise it contains description
//      of incompatibility.
std::pair<ESchemaCompatibility, TError> CheckTableSchemaCompatibility(
    const TTableSchema& inputSchema,
    const TTableSchema& outputSchema,
    bool ignoreSortOrder);

//! Compared to #ValidateTableSchema, additionally validates
//! aggregated and computed columns (this involves calling some heavy QL-related
//! stuff which is missing in yt/client).
void ValidateTableSchemaHeavy(
    const TTableSchema& schema,
    bool isTableDynamic);

//! Validates computed columns.
//!
//! Validates that:
//! - Type of a computed column matches the type of its expression.
//! - All referenced columns appear in schema and are not computed.
//! For dynamic tables, additionally validates that all computed and referenced
//! columns are key columns.
void ValidateComputedColumns(
    const TTableSchema& schema,
    bool isTableDynamic);

//! Validates that all computed columns in the outputSchema are present in the
//! inputSchema and have the same expression.
TError ValidateComputedColumnsCompatibility(
    const TTableSchema& inputSchema,
    const TTableSchema& outputSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
