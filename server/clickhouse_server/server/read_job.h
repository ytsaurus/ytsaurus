#pragma once

#include "read_job_spec.h"

#include <yt/server/clickhouse_server/interop/api.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/core/concurrency/public.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

TReadJobSpec LoadReadJobSpec(const TString& spec);

////////////////////////////////////////////////////////////////////////////////

NInterop::TTableReaderList CreateJobTableReaders(
    const NApi::NNative::IClientPtr& client,
    const TString& jobSpec,
    const NInterop::TStringList& columns,
    const NInterop::TSystemColumns& systemColumns,
    const NConcurrency::IThroughputThrottlerPtr throttler,
    size_t maxStreamCount,
    const NInterop::TTableReaderOptions& options);

}   // namespace NClickHouse
}   // namespace NYT
