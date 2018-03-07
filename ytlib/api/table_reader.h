#pragma once

#include "client.h"
#include "native_client.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/ypath/public.h>

#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

TFuture<NTableClient::ISchemalessMultiChunkReaderPtr> CreateTableReader(
    INativeClientPtr client,
    const NYPath::TRichYPath& path,
    const TTableReaderOptions& options,
    NTableClient::TNameTablePtr nameTable,
    const NTableClient::TColumnFilter& columnFilter = NTableClient::TColumnFilter(),
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

NConcurrency::IAsyncZeroCopyInputStreamPtr CreateBlobTableReader(
    NTableClient::ISchemalessChunkReaderPtr reader,
    const TNullable<TString>& partIndexColumnName,
    const TNullable<TString>& dataColumnName,
    i64 startPartIndex,
    const TNullable<i64>& offset = Null,
    const TNullable<i64>& partSize = Null);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
