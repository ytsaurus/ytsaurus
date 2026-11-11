#pragma once

#include "public.h"

#include <yt/yt/client/formats/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NRowIOBenchmark {

////////////////////////////////////////////////////////////////////////////////

TSharedRef PrepareInputStream(
    const NFormats::TFormat& format,
    const TDataset& dataset,
    int batchRowCount,
    NFormats::TArrowWriterStatisticsPtr statistics = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
