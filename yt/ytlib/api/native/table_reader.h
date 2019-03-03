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
};

TFuture<TSchemalessMultiChunkReaderCreateResult> CreateSchemalessMultiChunkReader(
    const IClientPtr& client,
    const NYPath::TRichYPath& richPath,
    const TTableReaderOptions& options,
    NTableClient::TNameTablePtr nameTable,
    const NTableClient::TColumnFilter& columnFilter = {},
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler());

NConcurrency::IAsyncZeroCopyInputStreamPtr CreateBlobTableReader(
    ITableReaderPtr reader,
    const std::optional<TString>& partIndexColumnName,
    const std::optional<TString>& dataColumnName,
    i64 startPartIndex,
    std::optional<i64> offset = std::nullopt,
    std::optional<i64> partSize = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
