#pragma once

#include "private.h"

#include <yt/yt/client/table_client/public.h>

#include <Storages/MergeTree/KeyCondition.h>
#include <Core/NamesAndTypes.h>
#include <Core/Block.h>
#include <Core/Field.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

// YT logical type system -> CH data type system.

DB::DataTypePtr ToDataType(
    const NTableClient::TComplexTypeFieldDescriptor& descriptor,
    const TCompositeSettingsPtr& settings,
    bool enableReadOnlyConversions = true);

DB::DataTypes ToDataTypes(
    const NTableClient::TTableSchema& schema,
    const TCompositeSettingsPtr& settings,
    bool enableReadOnlyConversions = true);

DB::NamesAndTypesList ToNamesAndTypesList(const NTableClient::TTableSchema& schema, const TCompositeSettingsPtr& settings);

DB::Block ToHeaderBlock(const NTableClient::TTableSchema& schema, const TCompositeSettingsPtr& settings);

////////////////////////////////////////////////////////////////////////////////

NTableClient::TLogicalTypePtr ToLogicalType(const DB::DataTypePtr& type, const TCompositeSettingsPtr& settings);

NTableClient::TTableSchema ToTableSchema(
    const DB::ColumnsDescription& columns,
    const NTableClient::TKeyColumns& keyColumns,
    const TCompositeSettingsPtr& settings);

////////////////////////////////////////////////////////////////////////////////

//! Convert single YT-value (UnversionedValue) to CH-value (DB::Field).
//! This function is suboptimal and can be used only for query preparation.
//! For converting data on execution stage consider using TYTCHConverter.
DB::Field ToField(
    const NTableClient::TUnversionedValue& value,
    const NTableClient::TLogicalTypePtr& type);

//! Convert YT-row from the table to vector of CH-values (DB::Field).
//! ToField is used to convert every value, so it's suboptimal and should be
//! used only for query preparation as well.
std::vector<DB::Field> UnversionedRowToFields(
    const NTableClient::TUnversionedRow& row,
    const NTableClient::TTableSchema& schema);

// TODO(max42): pass logical type.
void ToUnversionedValue(const DB::Field& field, NTableClient::TUnversionedValue* value);

////////////////////////////////////////////////////////////////////////////////

DB::Block ToBlock(
    const NTableClient::IUnversionedRowBatchPtr& batch,
    const NTableClient::TTableSchema& readSchema,
    const std::vector<int>& idToColumnIndex,
    const DB::Block& headerBlock,
    const TCompositeSettingsPtr& compositeSettings);

TSharedRange<NTableClient::TUnversionedRow> ToRowRange(
    const DB::Block& block,
    const std::vector<DB::DataTypePtr>& dataTypes,
    const std::vector<int>& columnIndexToId,
    const TCompositeSettingsPtr& settings);

////////////////////////////////////////////////////////////////////////////////

struct TClickHouseKeys
{
    // Both keys are inclusive (ClickHouse-notation).
    std::vector<DB::FieldRef> MinKey;
    std::vector<DB::FieldRef> MaxKey;
};

////////////////////////////////////////////////////////////////////////////////

//! Converts chunk keys from YT format to CH format.
//! Since CH does not support Nullable types in primary key, there are some tricks
//! to eliminate Null values. These tricks can lead to key bounds expansion.
//! (more detailed explanation of the tricks is given in comments near implementation)
//! dataTypes - actual data types of the columns. Can be nullable.
//! usedKeyColumnCount - hint about how many columns are used in KeyCondition.
//! Returned CH-keys always have usedKeyColumnCount columns fro non-empty key.
//! tryMakeBoundsInclusive - try to convert exclusive bounds to inclusive.
TClickHouseKeys ToClickHouseKeys(
    const NTableClient::TKeyBound& ytLowerBound,
    const NTableClient::TKeyBound& ytUpperBound,
    const NTableClient::TTableSchema& schema,
    const DB::DataTypes& dataTypes,
    int usedKeyColumnCount,
    bool tryMakeBoundsInclusive = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

