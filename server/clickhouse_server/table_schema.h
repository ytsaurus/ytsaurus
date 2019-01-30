#pragma once

#include <yt/server/clickhouse_server/public.h>

#include <Core/NamesAndTypes.h>
#include <Core/SortDescription.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseTableSchema
{
public:
    const DB::NamesAndTypesList Columns;
    const DB::NamesAndTypesList KeyColumns;
    const DB::Names PrimarySortColumns;

public:
    TClickHouseTableSchema(
        DB::NamesAndTypesList columns,
        DB::NamesAndTypesList keyColumns,
        DB::Names primarySortColumns);

    bool HasPrimaryKey() const
    {
        return !KeyColumns.empty();
    }

    DB::DataTypes GetKeyDataTypes() const;

    static TClickHouseTableSchema From(const TTable& table);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

