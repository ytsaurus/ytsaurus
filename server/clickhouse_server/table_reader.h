#pragma once

#include "private.h"

#include "column_builder.h"
#include "system_columns.h"
#include "table_schema.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/client/table_client/row_base.h>

#include <yt/client/api/public.h>

#include <yt/client/api/client.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct ITableReader
{
    virtual ~ITableReader() = default;

    virtual std::vector<TClickHouseColumn> GetColumns() const = 0;

    virtual const std::vector<TClickHouseTablePtr>& GetTables() const = 0;

    /// Reads bunch of values and appends it to the column buffers.
    virtual bool Read(const TColumnBuilderList& columns) = 0;
};

using TTableReaderList = std::vector<ITableReaderPtr>;

////////////////////////////////////////////////////////////////////////////////

ITableReaderPtr CreateTableReader(
    std::vector<TClickHouseTablePtr> tables,
    std::vector<TClickHouseColumn> columns,
    TSystemColumns systemColumns,
    NTableClient::ISchemafulReaderPtr chunkReader);

// Just read single table
ITableReaderPtr CreateTableReader(
    TClickHouseTablePtr table,
    NTableClient::ISchemafulReaderPtr chunkReader);

ITableReaderPtr CreateTableReader(
    const NApi::NNative::IClientPtr& client,
    const NYPath::TRichYPath& path,
    bool unordered,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemafulReaderPtr CreateSchemafulTableReader(
    const NApi::NNative::IClientPtr& client,
    const NYPath::TRichYPath& path,
    const NTableClient::TTableSchema& schema,
    const NApi::TTableReaderOptions& options,
    const NTableClient::TColumnFilter& columnFilter = NTableClient::TColumnFilter());

////////////////////////////////////////////////////////////////////////////////



} // namespace NYT::NClickHouseServer
