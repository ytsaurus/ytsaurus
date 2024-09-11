#pragma once

#include "private.h"

#include "yt_to_ch_block_converter.h"

#include <yt/yt/ytlib/table_client/public.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/statistics.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockStream_fwd.h>

#include <Storages/SelectQueryInfo.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TBlockInputStream
    : public DB::IBlockInputStream
{
public:
    DEFINE_BYREF_RO_PROPERTY(NTableClient::ISchemalessMultiChunkReaderPtr, Reader);

public:
    TBlockInputStream(
        NTableClient::ISchemalessMultiChunkReaderPtr reader,
        TReadPlanWithFilterPtr readPlanWithFilter,
        NTracing::TTraceContextPtr traceContext,
        THost* host,
        TQuerySettingsPtr settings,
        NLogging::TLogger logger,
        NChunkClient::TChunkReaderStatisticsPtr chunkReaderStatistics,
        TCallback<void(const TStatistics&)> statisticsCallback);

    virtual std::string getName() const override;
    virtual DB::Block getHeader() const override;

    virtual void readPrefixImpl() override;
    virtual void readSuffixImpl() override;

private:
    const TReadPlanWithFilterPtr ReadPlan_;
    NTracing::TTraceContextPtr TraceContext_;
    THost* const Host_;
    const TQuerySettingsPtr Settings_;
    const NLogging::TLogger Logger;
    const NTableClient::TRowBufferPtr RowBuffer_;

    //! Converters for every step from the read plan.
    //! Every converter converts only additional columns required by corresponding step.
    std::vector<TYTToCHBlockConverter> Converters_;
    //! Output header block after execution of all read steps and filter actions (if any).
    DB::Block HeaderBlock_;

    TDuration ColumnarConversionCpuTime_;
    TDuration NonColumnarConversionCpuTime_;
    TDuration ConversionSyncWaitTime_;

    TDuration WaitReadyEventTime_;

    NProfiling::TWallTimer IdleTimer_ = NProfiling::TWallTimer(false /*start*/);

    int ReadCount_ = 0;

    NChunkClient::TChunkReaderStatisticsPtr ChunkReaderStatistics_;
    TStatistics Statistics_;
    TCallback<void(const TStatistics&)> StatisticsCallback_;

    virtual DB::Block readImpl() override;
    void Prepare();

    DB::Block ConvertStepColumns(
        int stepIndex,
        const NTableClient::IUnversionedRowBatchPtr& batch,
        TRange<DB::UInt8> filterHint);
    DB::Block DoConvertStepColumns(
        int stepIndex,
        const NTableClient::IUnversionedRowBatchPtr& batch,
        TRange<DB::UInt8> filterHint);
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TBlockInputStream> CreateBlockInputStream(
    NTableClient::ISchemalessMultiChunkReaderPtr reader,
    TReadPlanWithFilterPtr readPlan,
    NTracing::TTraceContextPtr traceContext,
    THost* host,
    TQuerySettingsPtr settings,
    NLogging::TLogger logger,
    NChunkClient::TChunkReaderStatisticsPtr chunkReaderStatistics,
    TCallback<void(const TStatistics&)> statisticsCallback);

std::shared_ptr<TBlockInputStream> CreateBlockInputStream(
    TStorageContext* storageContext,
    const TSubquerySpec& subquerySpec,
    TReadPlanWithFilterPtr readPlan,
    const NTracing::TTraceContextPtr& traceContext,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    NTableClient::IGranuleFilterPtr granuleFilter,
    TCallback<void(const TStatistics&)> statisticsCallback);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
