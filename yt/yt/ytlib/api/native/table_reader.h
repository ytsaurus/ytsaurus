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
    const NTableClient::TColumnFilter& columnFilter = {},
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler());

struct TSchemalessMultiChunkReaderCreateResult
{
    NTableClient::ISchemalessMultiChunkReaderPtr Reader;
    std::vector<TString> OmittedInaccessibleColumns;
    NTableClient::TTableSchema TableSchema;
};

TFuture<TSchemalessMultiChunkReaderCreateResult> CreateSchemalessMultiChunkReader(
    const IClientPtr& client,
    const NYPath::TRichYPath& richPath,
    const TTableReaderOptions& options,
    const NTableClient::TNameTablePtr& nameTable,
    const NTableClient::TColumnFilter& columnFilter = {},
    const NConcurrency::IThroughputThrottlerPtr& bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    const NConcurrency::IThroughputThrottlerPtr& rpsThrottler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
