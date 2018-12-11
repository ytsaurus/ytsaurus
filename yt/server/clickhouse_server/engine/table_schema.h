#pragma once

#include "clickhouse.h"

#include <yt/server/clickhouse_server/native/public.h>

//#include <Core/NamesAndTypes.h>
//#include <Core/SortDescription.h>

namespace NYT::NClickHouseServer::NEngine {

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

    static TTableSchema From(const NNative::TTable& table);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine

