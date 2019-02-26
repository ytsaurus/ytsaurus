#pragma once

#include "read_job_spec.h"
#include "system_columns.h"
#include "table_reader.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/core/concurrency/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TReadJobSpec LoadReadJobSpec(const TString& spec);

////////////////////////////////////////////////////////////////////////////////

TTableReaderList CreateJobTableReaders(
    const NApi::NNative::IClientPtr& client,
    const TString& jobSpec,
    const std::vector<TString>& columns,
    const TSystemColumns& systemColumns,
    size_t maxStreamCount,
    bool unordered);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
