#pragma once

#include "public.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/client/api/client.h>

#include <yt/client/ypath/public.h>

#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

TFuture<ITableReaderPtr> CreateTableReader(
    IClientPtr client,
    const NYPath::TRichYPath& path,
    const TTableReaderOptions& options,
    NTableClient::TNameTablePtr nameTable,
    const NTableClient::TColumnFilter& columnFilter = NTableClient::TColumnFilter(),
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler());

TFuture<NTableClient::ISchemalessMultiChunkReaderPtr> CreateSchemalessMultiChunkReader(
    IClientPtr client,
    const NYPath::TRichYPath& richPath,
    const TTableReaderOptions& options,
    NTableClient::TNameTablePtr nameTable,
    const NTableClient::TColumnFilter& columnFilter = NTableClient::TColumnFilter(),
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
