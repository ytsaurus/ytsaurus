#include "virtual_columns.h"

#include "db_helpers.h"

#include <yt/server/clickhouse_server/system_columns.h>

#include <Core/Names.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

const DB::NamesAndTypesList& ListSystemVirtualColumns()
{
    static const DB::NamesAndTypesList columns = {
        {"_table", GetDataType("String")}
    };
    return columns;
}

TSystemColumns GetSystemColumns(const DB::Names& virtual_)
{
    DB::NameSet names(virtual_.begin(), virtual_.end());

    TSystemColumns systemColumns;

    if (names.find("_table") != names.end()) {
        systemColumns.TableName = "_table";
    }

    return systemColumns;
}

} // namespace NYT::NClickHouseServer
