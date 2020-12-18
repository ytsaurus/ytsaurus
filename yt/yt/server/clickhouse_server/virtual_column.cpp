#include "virtual_column.h"

#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

const TString VirtualColumnNamePrefix = "$";
const TString TableIndexColumnName = VirtualColumnNamePrefix + "table_index";
const TString TableKeyColumnName = VirtualColumnNamePrefix + "table_name";
const TString TablePathColumnName = VirtualColumnNamePrefix + "table_path";

const DB::NamesAndTypesList VirtualColumnNamesAndTypes{
    DB::NameAndTypePair(TableIndexColumnName, std::make_shared<DB::DataTypeInt64>()),
    DB::NameAndTypePair(TableKeyColumnName, std::make_shared<DB::DataTypeString>()),
    DB::NameAndTypePair(TablePathColumnName, std::make_shared<DB::DataTypeString>()),
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
