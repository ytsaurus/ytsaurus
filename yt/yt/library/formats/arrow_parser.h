#pragma once

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/config.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

arrow::Status DecodeRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch, NTableClient::IValueConsumer* consumer);

std::unique_ptr<IParser> CreateParserForArrow(NTableClient::IValueConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
