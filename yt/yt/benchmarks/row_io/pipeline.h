#pragma once

#include <yt/yt/benchmarks/row_io/dataset/public.h>

#include <yt/yt/client/formats/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NRowIOBenchmark {

////////////////////////////////////////////////////////////////////////////////

struct TRunResult
{
    double Seconds = 0.0;
    i64 OutputBytes = 0;
    int OutputBlockCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

TRunResult RunIteration(
    const NFormats::TFormat& format,
    const TDataset& dataset,
    TRef input,
    bool validate);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
