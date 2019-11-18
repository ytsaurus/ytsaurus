#pragma once

#include "private.h"

#include <yt/client/table_client/public.h>

#include <Core/NamesAndTypes.h>
#include <Core/SortDescription.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseTableSchema
{
public:
    DB::NamesAndTypesList Columns;
    DB::NamesAndTypesList KeyColumns;
    DB::Names PrimarySortColumns;

public:
    TClickHouseTableSchema(
        DB::NamesAndTypesList columns,
        DB::NamesAndTypesList keyColumns,
        DB::Names primarySortColumns);

    TClickHouseTableSchema() = default;

    bool HasPrimaryKey() const
    {
        return !KeyColumns.empty();
    }

    DB::DataTypes GetKeyDataTypes() const;

    static TClickHouseTableSchema From(const TClickHouseTable& table);
    static TClickHouseTableSchema From(const NTableClient::TTableSchema& schema);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

