#pragma once

#include <yt/server/clickhouse_server/public.h>

#include <DataStreams/IBlockInputStream.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::BlockInputStreamPtr CreateStorageInputStream(ITableReaderPtr tableReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
