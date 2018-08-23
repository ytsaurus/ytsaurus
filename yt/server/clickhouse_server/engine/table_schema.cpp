#include "table_schema.h"

#include "db_helpers.h"

#include <DataTypes/DataTypeFactory.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

TTableSchema::TTableSchema(DB::NamesAndTypesList columns,
                           DB::NamesAndTypesList keyColumns,
                           DB::Names primarySortColumns)
    : Columns(std::move(columns))
    , KeyColumns(std::move(keyColumns))
    , PrimarySortColumns(std::move(primarySortColumns))
{}

DB::DataTypes TTableSchema::GetKeyDataTypes() const
{
    DB::DataTypes dataTypes;
    dataTypes.reserve(KeyColumns.size());

    for (const auto& column: KeyColumns) {
        dataTypes.push_back(column.type);
    }

    return dataTypes;
}

TTableSchema TTableSchema::From(const NInterop::TTable& table)
{
    const auto& dataTypes = DB::DataTypeFactory::instance();

    DB::NamesAndTypesList columns;
    DB::NamesAndTypesList keyColumns;
    DB::Names primarySortColumns;

    for (const auto& column: table.Columns) {
        auto dataType = dataTypes.get(GetTypeName(column));
        columns.emplace_back(column.Name, dataType);

        if (column.IsSorted()) {
            keyColumns.emplace_back(column.Name, dataType);
            primarySortColumns.emplace_back(column.Name);
        }
    }

    return TTableSchema(
        std::move(columns),
        std::move(keyColumns),
        std::move(primarySortColumns));
}

} // namespace NClickHouse
} // namespace NYT
