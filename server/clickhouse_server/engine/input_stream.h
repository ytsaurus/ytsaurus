#pragma once

#include <yt/server/clickhouse_server/native/public.h>

#include <DataStreams/IBlockInputStream.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

DB::BlockInputStreamPtr CreateStorageInputStream(NNative::ITableReaderPtr tableReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
