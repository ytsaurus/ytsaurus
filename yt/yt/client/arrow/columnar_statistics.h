#pragma once

#include <yt/yt/client/table_client/public.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/metadata.h>

namespace NYT::NArrow {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TColumnarStatistics ExtractColumnarStatistics(
    parquet::FileMetaData& parquetFileMeta);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
