#pragma once

#include <yt/server/clickhouse/interop/api.h>

#include <DataStreams/IBlockInputStream.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

DB::BlockInputStreamPtr CreateStorageInputStream(
    NInterop::ITableReaderPtr tableReader);

}   // namespace NClickHouse
}   // namespace NYT
