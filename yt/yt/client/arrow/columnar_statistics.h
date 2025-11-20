#pragma once

#include <yt/yt/client/table_client/public.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow_next/cpp/src/parquet/metadata.h>

namespace NYT::NArrow {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TColumnarStatistics ExtractColumnarStatistics(
    const std::shared_ptr<arrow20::RecordBatch>& batch);

NTableClient::TColumnarStatistics ExtractColumnarStatistics(
    arrow20::Table& arrowTable);

NTableClient::TColumnarStatistics ExtractColumnarStatistics(
    parquet20::FileMetaData& parquetFileMeta);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
