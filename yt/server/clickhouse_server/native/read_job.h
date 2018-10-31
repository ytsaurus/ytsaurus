#pragma once

#include "read_job_spec.h"
#include "system_columns.h"
#include "table_reader.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/core/concurrency/public.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

TReadJobSpec LoadReadJobSpec(const TString& spec);

////////////////////////////////////////////////////////////////////////////////

TTableReaderList CreateJobTableReaders(
    const NApi::NNative::IClientPtr& client,
    const TString& jobSpec,
    const std::vector<TString>& columns,
    const TSystemColumns& systemColumns,
    const NConcurrency::IThroughputThrottlerPtr throttler,
    size_t maxStreamCount,
    const TTableReaderOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
