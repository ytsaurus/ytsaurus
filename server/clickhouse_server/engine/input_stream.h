#pragma once

#include <yt/server/clickhouse_server/native/public.h>

#include <DataStreams/IBlockInputStream.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

DB::BlockInputStreamPtr CreateStorageInputStream(NNative::ITableReaderPtr tableReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
