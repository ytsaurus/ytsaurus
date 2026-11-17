#pragma once

#include "public.h"

#include <yt/yt/client/formats/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NRowIOBenchmark {

////////////////////////////////////////////////////////////////////////////////

enum class EArrowDictionaryEncoding
{
    Allowed,
    Disabled,
};

////////////////////////////////////////////////////////////////////////////////

TSharedRef PrepareInputStream(
    const NFormats::TFormat& format,
    const TDataset& dataset,
    int batchRowCount,
    NFormats::TArrowWriterStatisticsPtr statistics = nullptr,
    EArrowDictionaryEncoding dictionaryEncoding = EArrowDictionaryEncoding::Allowed);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
