#pragma once

#include "public.h"

#include "column_builder.h"
#include "system_columns.h"
#include "table_schema.h"

#include <yt/ytlib/table_client/public.h>

namespace NYT::NClickHouseServer::NNative {

////////////////////////////////////////////////////////////////////////////////

struct TTableReaderOptions
{
    bool Unordered = false;
};

////////////////////////////////////////////////////////////////////////////////

struct ITableReader
{
    virtual ~ITableReader() = default;

    virtual TColumnList GetColumns() const = 0;

    virtual const TTableList& GetTables() const = 0;

    /// Reads bunch of values and appends it to the column buffers.
    virtual bool Read(const TColumnBuilderList& columns) = 0;
};

using TTableReaderList = std::vector<ITableReaderPtr>;

////////////////////////////////////////////////////////////////////////////////

ITableReaderPtr CreateTableReader(
    TTableList tables,
    TColumnList columns,
    TSystemColumns systemColumns,
    NTableClient::ISchemafulReaderPtr chunkReader);

// Just read single table
ITableReaderPtr CreateTableReader(
    TTablePtr table,
    NTableClient::ISchemafulReaderPtr chunkReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NNative
