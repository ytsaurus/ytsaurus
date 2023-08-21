#pragma once

#include "private.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <DataStreams/IBlockInputStream.h>

#include <Storages/SelectQueryInfo.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::BlockInputStreamPtr CreatePrewhereBlockInputStream(
    TStorageContext* storageContext,
    const TSubquerySpec& subquerySpec,
    const std::vector<TString>& realColumns,
    const std::vector<TString>& virtualColumns,
    const NTracing::TTraceContextPtr& traceContext,
    std::vector<NChunkClient::TDataSliceDescriptor> dataSliceDescriptors,
    DB::PrewhereInfoPtr prewhereInfo);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
