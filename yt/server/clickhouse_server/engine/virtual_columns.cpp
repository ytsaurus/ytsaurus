#include "virtual_columns.h"

#include "db_helpers.h"

#include <Core/Names.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

const DB::NamesAndTypesList& ListSystemVirtualColumns()
{
    static const DB::NamesAndTypesList columns = {
        {"_table", GetDataType("String")}
    };
    return columns;
}

NInterop::TSystemColumns GetSystemColumns(const DB::Names& virtual_)
{
    DB::NameSet names(virtual_.begin(), virtual_.end());

    NInterop::TSystemColumns systemColumns;

    if (names.find("_table") != names.end()) {
        systemColumns.TableName = "_table";
    }

    return systemColumns;
}

} // namespace NClickHouse
} // namespace NYT
