#pragma once

#include <yt/cpp/mapreduce/interface/client.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/reader.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/arrow/writer.h>

namespace NYT::NFormats::NArrow {

////////////////////////////////////////////////////////////////////////////////

void ThrowOnError(const arrow::Status& status);

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<arrow::io::RandomAccessFile> CreateParquetAdapter(
    const TString* metaData,
    int startMetadataOffset,
    const std::shared_ptr<IInputStream>& reader = nullptr);

std::shared_ptr<arrow::Schema> CreateArrowSchemaFromParquetMetadata(const TString* metaData, int startIndex);

TTableSchema CreateYtTableSchemaFromArrowSchema(const std::shared_ptr<arrow::Schema>& arrowSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats::NArrow
