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
    bool isReadConversions = true);

DB::DataTypes ToDataTypes(
    const std::vector<NTableClient::TColumnSchema>& schemas,
    const TCompositeSettingsPtr& settings,
    bool isReadConversions = true);

DB::DataTypes ToDataTypes(
    const NTableClient::TTableSchema& schema,
    const TCompositeSettingsPtr& settings,
    bool isReadConversions = true);

DB::NamesAndTypesList ToNamesAndTypesList(const std::vector<NTableClient::TColumnSchema>& schemas, const TCompositeSettingsPtr& settings);

DB::NamesAndTypesList ToNamesAndTypesList(const NTableClient::TTableSchema& schema, const TCompositeSettingsPtr& settings);

DB::Block ToHeaderBlock(const std::vector<NTableClient::TColumnSchema>& schemas, const TCompositeSettingsPtr &settings);

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
//! For converting data on execution stage consider using TYTToCHColumnConverter.
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

std::vector<int> GetColumnIndexToId(const NTableClient::TNameTablePtr& nameTable, const std::vector<TString>& columnNames);

////////////////////////////////////////////////////////////////////////////////

TSharedRange<NTableClient::TUnversionedRow> ToRowRange(
    const DB::Block& block,
    const std::vector<DB::DataTypePtr>& dataTypes,
    const std::vector<int>& columnIndexToId,
    const TCompositeSettingsPtr& settings);

TSharedMutableRange<NTableClient::TMutableUnversionedRow> ToMutableRowRange(
    const DB::Block& block,
    const std::vector<DB::DataTypePtr>& dataTypes,
    const std::vector<int>& columnIndexItId,
    const TCompositeSettingsPtr& settings,
    int extraColumnCapacity = 0);

////////////////////////////////////////////////////////////////////////////////

struct TClickHouseKeys
{
    // Both keys are inclusive (ClickHouse-notation).
    std::vector<DB::FieldRef> MinKey;
    std::vector<DB::FieldRef> MaxKey;
};

////////////////////////////////////////////////////////////////////////////////

//! Converts chunk keys from YT format to CH format.
//! schema - schema with actual data types of the columns for proper conversion.
//! dataTypes - actual ClickHouse data types of the columns. Can be nullable.
//! usedKeyColumnCount - hint about how many columns are used in KeyCondition.
//! Returned CH-keys always have usedKeyColumnCount columns.
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

