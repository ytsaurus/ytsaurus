#pragma once

#include "public.h"

#include <yt/client/api/client.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/client/ypath/public.h>

#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NApi {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

TFuture<ITableReaderPtr> CreateTableReader(
    IClientPtr client,
    const NYPath::TRichYPath& path,
    const TTableReaderOptions& options,
    NTableClient::TNameTablePtr nameTable,
    const NTableClient::TColumnFilter& columnFilter = NTableClient::TColumnFilter(),
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

TFuture<NTableClient::ISchemalessMultiChunkReaderPtr> CreateSchemalessMultiChunkReader(
    IClientPtr client,
    const NYPath::TRichYPath& richPath,
    const TTableReaderOptions& options,
    NTableClient::TNameTablePtr nameTable,
    const NTableClient::TColumnFilter& columnFilter = NTableClient::TColumnFilter(),
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

NConcurrency::IAsyncZeroCopyInputStreamPtr CreateBlobTableReader(
    ITableReaderPtr reader,
    const TNullable<TString>& partIndexColumnName,
    const TNullable<TString>& dataColumnName,
    i64 startPartIndex,
    const TNullable<i64>& offset = Null,
    const TNullable<i64>& partSize = Null);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT
