#pragma once

#include "private.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <Processors/ISource.h>
#include <Storages/SelectQueryInfo.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::SourcePtr CreatePrewhereSecondaryQuerySource(
    TStorageContext* storageContext,
    const TSubquerySpec& subquerySpec,
    TReadPlanWithFilterPtr readPlan,
    const NTracing::TTraceContextPtr& traceContext,
    std::vector<NChunkClient::TDataSliceDescriptor> dataSliceDescriptors,
    NTableClient::IGranuleFilterPtr granuleFilter,
    TCallback<void(const TStatistics&)> statisticsCallback);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
