#pragma once

#include "private.h"

#include <yt/yt/ytlib/table_client/public.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/logging/log.h>

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
        NTableClient::TTableSchemaPtr readSchemaWithVirtualColumns,
        NTracing::TTraceContextPtr traceContext,
        THost* host,
        TQuerySettingsPtr settings,
        NLogging::TLogger logger,
        DB::PrewhereInfoPtr prewhereInfo);

    virtual std::string getName() const override;
    virtual DB::Block getHeader() const override;

    virtual void readPrefixImpl() override;
    virtual void readSuffixImpl() override;

private:
    const NTableClient::TTableSchemaPtr ReadSchemaWithVirtualColumns_;
    NTracing::TTraceContextPtr TraceContext_;
    THost* const Host_;
    const TQuerySettingsPtr Settings_;
    const NLogging::TLogger Logger;
    const NTableClient::TRowBufferPtr RowBuffer_;
    const DB::PrewhereInfoPtr PrewhereInfo_;
    DB::ExpressionActionsPtr PrewhereActions_;

    DB::Block InputHeaderBlock_;
    DB::Block OutputHeaderBlock_;
    std::vector<int> IdToColumnIndex_;

    TDuration ColumnarConversionCpuTime_;
    TDuration NonColumnarConversionCpuTime_;
    TDuration ConversionSyncWaitTime_;

    TDuration WaitReadyEventTime_;

    NProfiling::TWallTimer IdleTimer_ = NProfiling::TWallTimer(false /*start*/);

    int ReadCount_ = 0;

    virtual DB::Block readImpl() override;
    void Prepare();
    DB::Block ConvertRowBatchToBlock(const NTableClient::IUnversionedRowBatchPtr& batch);
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TBlockInputStream> CreateBlockInputStream(
    NTableClient::ISchemalessMultiChunkReaderPtr reader,
    NTableClient::TTableSchemaPtr readSchema,
    NTracing::TTraceContextPtr traceContext,
    THost* host,
    TQuerySettingsPtr settings,
    NLogging::TLogger logger,
    DB::PrewhereInfoPtr prewhereInfo);

std::shared_ptr<TBlockInputStream> CreateBlockInputStream(
    TStorageContext* storageContext,
    const TSubquerySpec& subquerySpec,
    const std::vector<TString>& realColumns,
    const std::vector<TString>& virtualColumns,
    const NTracing::TTraceContextPtr& traceContext,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    DB::PrewhereInfoPtr prewhereInfo,
    NTableClient::IGranuleFilterPtr granuleFilter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
