#pragma once

#include "private.h"

#include <yt/client/table_client/public.h>

#include <Core/NamesAndTypes.h>
#include <Core/Block.h>
#include <Core/Field.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::Names ToNames(const std::vector<TString>& columnNames);
std::vector<TString> ToVectorString(const DB::Names& columnNames);

////////////////////////////////////////////////////////////////////////////////

// YT logical type system -> CH data type system.

DB::DataTypePtr ToDataType(const NTableClient::TComplexTypeFieldDescriptor& descriptor, const TCompositeSettingsPtr& settings);

DB::DataTypes ToDataTypes(const NTableClient::TTableSchema& schema, const TCompositeSettingsPtr& settings);

DB::NamesAndTypesList ToNamesAndTypesList(const NTableClient::TTableSchema& schema, const TCompositeSettingsPtr& settings);

DB::Block ToHeaderBlock(const NTableClient::TTableSchema& schema, const TCompositeSettingsPtr& settings);

////////////////////////////////////////////////////////////////////////////////

// YT physical type system -> CH field type system.

NTableClient::EValueType ToValueType(DB::Field::Types::Which which);

////////////////////////////////////////////////////////////////////////////////

// CH logicl type system -> YT type system.

NTableClient::TTableSchema ConvertToTableSchema(
    const DB::ColumnsDescription& columns,
    const NTableClient::TKeyColumns& keyColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

