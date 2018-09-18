#pragma once

#include <yt/server/clickhouse_server/interop/api.h>

#include <Core/NamesAndTypes.h>
#include <Core/SortDescription.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TTableSchema
{
public:
    const DB::NamesAndTypesList Columns;
    const DB::NamesAndTypesList KeyColumns;
    const DB::Names PrimarySortColumns;

public:
    TTableSchema(
        DB::NamesAndTypesList columns,
        DB::NamesAndTypesList keyColumns,
        DB::Names primarySortColumns);

    bool HasPrimaryKey() const
    {
        return !KeyColumns.empty();
    }

    DB::DataTypes GetKeyDataTypes() const;

    static TTableSchema From(const NInterop::TTable& table);
};

} // namespace NClickHouse
} // namespace NYT

