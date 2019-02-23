#pragma once

#include "private.h"

#include "column_builder.h"
#include "system_columns.h"
#include "table_schema.h"

#include <yt/ytlib/table_client/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TTableReaderOptions
{
    bool Unordered = false;
};

////////////////////////////////////////////////////////////////////////////////

struct ITableReader
{
    virtual ~ITableReader() = default;

    virtual std::vector<TColumn> GetColumns() const = 0;

    virtual const std::vector<TTablePtr>& GetTables() const = 0;

    /// Reads bunch of values and appends it to the column buffers.
    virtual bool Read(const TColumnBuilderList& columns) = 0;
};

using TTableReaderList = std::vector<ITableReaderPtr>;

////////////////////////////////////////////////////////////////////////////////

ITableReaderPtr CreateTableReader(
    std::vector<TTablePtr> tables,
    std::vector<TColumn> columns,
    TSystemColumns systemColumns,
    NTableClient::ISchemafulReaderPtr chunkReader);

// Just read single table
ITableReaderPtr CreateTableReader(
    TTablePtr table,
    NTableClient::ISchemafulReaderPtr chunkReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
