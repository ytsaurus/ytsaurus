#pragma once

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/config.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/record_batch.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

struct TArrowParserOptions
{
    //! Caps Arrow internal allocations and record-batch size checks.
    //! std::nullopt means no limit. Set to a reasonable value (e.g. 512 MB) in
    //! fuzz tests to convert OOM into a catchable exception.
    std::optional<i64> MaxAllocationBytes;
};

//! Decodes an already materialized Arrow record batch into YT values.
//! This is used by readers for externally attached Arrow-based formats.
arrow20::Status DecodeRecordBatch(
    const std::shared_ptr<arrow20::RecordBatch>& batch,
    NTableClient::IValueConsumer* consumer,
    const TArrowParserOptions& options = {});

std::unique_ptr<IParser> CreateParserForArrow(
    NTableClient::IValueConsumer* consumer,
    const TArrowParserOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
