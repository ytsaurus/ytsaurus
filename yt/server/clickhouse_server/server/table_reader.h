#pragma once

#include "public.h"

#include <yt/server/clickhouse_server/interop/api.h>

#include <yt/ytlib/table_client/public.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

NInterop::ITableReaderPtr CreateTablesReader(
    NInterop::TTableList tables,
    NInterop::TColumnList columns,
    NInterop::TSystemColumns systemColumns,
    NTableClient::ISchemafulReaderPtr chunkReader);

// Just read single table
NInterop::ITableReaderPtr CreateTableReader(
    NInterop::TTablePtr table,
    NTableClient::ISchemafulReaderPtr chunkReader);

}   // namespace NClickHouse
}   // namespace NYT
