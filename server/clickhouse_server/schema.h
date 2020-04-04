#pragma once

#include "private.h"

#include <yt/client/table_client/public.h>

#include <Core/NamesAndTypes.h>
#include <Core/Block.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::Names ToNames(const std::vector<TString>& columnNames);
std::vector<TString> ToVectorString(const DB::Names& columnNames);

////////////////////////////////////////////////////////////////////////////////

// YT type system -> CH type system.

DB::DataTypes ToDataTypes(const NTableClient::TTableSchema& schema);

DB::NamesAndTypesList ToNamesAndTypesList(const NTableClient::TTableSchema& schema);

DB::Block ToHeaderBlock(const NTableClient::TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

// CH type system -> YT type system.

NTableClient::TTableSchema ConvertToTableSchema(
    const DB::ColumnsDescription& columns,
    const NTableClient::TKeyColumns& keyColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

