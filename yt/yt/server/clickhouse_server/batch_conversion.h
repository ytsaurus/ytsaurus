#pragma once

#include "private.h"

#include <yt/client/table_client/public.h>

#include <clickhouse/src/Core/Block.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::Block ConvertRowBatchToBlock(
    const NTableClient::IUnversionedRowBatchPtr& batch,
    const NTableClient::TTableSchema& readSchema,
    const std::vector<int>& idToColumnIndex,
    const NTableClient::TRowBufferPtr& rowBuffer,
    const DB::Block& headerBlock);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
