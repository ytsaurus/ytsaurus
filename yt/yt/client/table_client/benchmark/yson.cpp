#include "common_benchmarks.h"

#include <yt/yt/experiments/random_row/wild_schema_catalog.h>

using namespace NYT::NTableClientBenchmark;
using namespace NYT::NWildSchemaCatalog;

////////////////////////////////////////////////////////////////////////////////

YT_BENCHMARK_FORMAT_ALL_MESSAGES(FormatWriterBenchmark, EBenchmarkedFormat::Yson);
YT_BENCHMARK_FORMAT_ALL_MESSAGES(FormatReaderBenchmark, EBenchmarkedFormat::Yson);

////////////////////////////////////////////////////////////////////////////////
