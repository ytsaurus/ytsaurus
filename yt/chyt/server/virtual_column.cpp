#include "virtual_column.h"

#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/VirtualColumnsDescription.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

const TString VirtualColumnNamePrefix = "$";
const TString TableIndexColumnName = VirtualColumnNamePrefix + "table_index";
const TString TableKeyColumnName = VirtualColumnNamePrefix + "table_name";
const TString TablePathColumnName = VirtualColumnNamePrefix + "table_path";

const DB::VirtualColumnsDescription VirtualColumns = []() {
    DB::VirtualColumnsDescription virtualColumns;

    virtualColumns.addPersistent(TableIndexColumnName, std::make_shared<DB::DataTypeInt64>(), /*codec*/ nullptr, /*comment*/ "");
    virtualColumns.addPersistent(TableKeyColumnName, std::make_shared<DB::DataTypeString>(), /*codec*/ nullptr, /*comment*/ "");
    virtualColumns.addPersistent(TablePathColumnName, std::make_shared<DB::DataTypeString>(), /*codec*/ nullptr, /*comment*/ "");

    return virtualColumns;
}();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
