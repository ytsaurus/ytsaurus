#include "table_schema.h"

#include "db_helpers.h"

#include "table.h"

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TClickHouseTableSchema::TClickHouseTableSchema(
    DB::NamesAndTypesList columns,
    DB::NamesAndTypesList keyColumns,
    DB::Names primarySortColumns)
    : Columns(std::move(columns))
    , KeyColumns(std::move(keyColumns))
    , PrimarySortColumns(std::move(primarySortColumns))
{}

DB::DataTypes TClickHouseTableSchema::GetKeyDataTypes() const
{
    DB::DataTypes dataTypes;
    dataTypes.reserve(KeyColumns.size());

    for (const auto& column: KeyColumns) {
        dataTypes.push_back(column.type);
    }

    return dataTypes;
}

TClickHouseTableSchema TClickHouseTableSchema::From(const TClickHouseTable& table)
{
    const auto& dataTypes = DB::DataTypeFactory::instance();

    DB::NamesAndTypesList columns;
    DB::NamesAndTypesList keyColumns;
    DB::Names primarySortColumns;

    for (const auto& column: table.Columns) {
        auto dataType = dataTypes.get(GetTypeName(column));

        if (column.IsNullable()) {
            dataType = makeNullable(dataType);
        }

        columns.emplace_back(column.Name, dataType);

        if (column.IsSorted()) {
            keyColumns.emplace_back(column.Name, dataType);
            primarySortColumns.emplace_back(column.Name);
        }
    }

    return TClickHouseTableSchema(
        std::move(columns),
        std::move(keyColumns),
        std::move(primarySortColumns));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
