#pragma once

#include "private.h"

#include "table.h"
#include "table_schema.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TSystemColumns
{
    std::optional<TString> TableName;

    size_t GetCount() const
    {
        return TableName ? 1 : 0;
    }

    std::vector<TColumn> ToColumnList() const
    {
        std::vector<TColumn> columns;
        columns.reserve(GetCount());

        if (TableName) {
            columns.emplace_back(*TableName, EClickHouseColumnType::String);
        }

        return columns;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

