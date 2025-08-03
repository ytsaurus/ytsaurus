#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

TFuture<ITableReaderPtr> CreateTableReader(
    IClientPtr client,
    const NYPath::TRichYPath& path,
    const TTableReaderOptions& options,
    NTableClient::TNameTablePtr nameTable,
    const NTableClient::TColumnFilter& columnFilter = {},
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler(),
    IMemoryUsageTrackerPtr readTableMemoryTracker = GetNullMemoryUsageTracker());

IRowBatchReaderPtr ToApiRowBatchReader(NTableClient::ISchemalessMultiChunkReaderPtr reader);
NTableClient::TTableReaderOptionsPtr ToInternalTableReaderOptions(const TTableReaderOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
