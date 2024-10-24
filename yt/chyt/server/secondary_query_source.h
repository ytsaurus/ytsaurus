#pragma once

#include "private.h"

#include <yt/yt/ytlib/table_client/public.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/statistics.h>

#include <Processors/ISource.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TClientChunkReadOptions CreateChunkReadOptions(
    const TString& user,
    NTableClient::IGranuleFilterPtr granuleFilter);

NTableClient::ISchemalessMultiChunkReaderPtr CreateSourceReader(
    TStorageContext* storageContext,
    const TSubquerySpec& subquerySpec,
    TReadPlanWithFilterPtr readPlan,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors);

////////////////////////////////////////////////////////////////////////////////

DB::SourcePtr CreateSecondaryQuerySource(
    NTableClient::ISchemalessMultiChunkReaderPtr reader,
    TReadPlanWithFilterPtr readPlan,
    NTracing::TTraceContextPtr traceContext,
    THost* host,
    TQuerySettingsPtr settings,
    NLogging::TLogger logger,
    NChunkClient::TChunkReaderStatisticsPtr chunkReaderStatistics,
    TCallback<void(const TStatistics&)> statisticsCallback);

DB::SourcePtr CreateSecondaryQuerySource(
    TStorageContext* storageContext,
    const TSubquerySpec& subquerySpec,
    TReadPlanWithFilterPtr readPlan,
    const NTracing::TTraceContextPtr& traceContext,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    NTableClient::IGranuleFilterPtr granuleFilter,
    TCallback<void(const TStatistics&)> statisticsCallback);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
