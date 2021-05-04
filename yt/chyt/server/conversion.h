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

NTableClient::TTableSchema ToTableSchema(
    const DB::ColumnsDescription& columns,
    const NTableClient::TKeyColumns& keyColumns,
    const TCompositeSettingsPtr& settings);

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): pass logical type.
DB::Field ToField(const NTableClient::TUnversionedValue& value);

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
    // Both are inclusive.
    std::vector<DB::FieldRef> MinKey;
    std::vector<DB::FieldRef> MaxKey;
};

////////////////////////////////////////////////////////////////////////////////

TClickHouseKeys ToClickHouseKeys(
    const NTableClient::TLegacyKey& ytLowerKey,
    const NTableClient::TLegacyKey& ytUpperKey,
    // How many columns are used in key condition.
    int usedKeyColumnCount,
    const DB::DataTypes& dataTypes,
    bool makeUpperBoundInclusive = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

