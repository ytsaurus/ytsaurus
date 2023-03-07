#pragma once

#include "private.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <DataStreams/IBlockInputStream.h>

#include <Storages/SelectQueryInfo.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::BlockInputStreamPtr CreatePrewhereBlockInputStream(
    TQueryContext* queryContext,
    const TSubquerySpec& subquerySpec,
    const DB::Names& columnNames,
    const NTracing::TTraceContextPtr& traceContext,
    std::vector<NChunkClient::TDataSliceDescriptor> dataSliceDescriptors,
    DB::PrewhereInfoPtr prewhereInfo);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
