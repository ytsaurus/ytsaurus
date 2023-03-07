#pragma once

#include "private.h"

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/client/table_client/schema.h>

#include <yt/core/logging/log.h>

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
        NTableClient::TTableSchema readSchema,
        NTracing::TTraceContextPtr traceContext,
        THost* host,
        NLogging::TLogger logger,
        DB::PrewhereInfoPtr prewhereInfo);

    virtual std::string getName() const override;

    virtual DB::Block getHeader() const override;

    virtual void readPrefixImpl() override;

    virtual void readSuffixImpl() override;

private:
    NTableClient::TTableSchema ReadSchema_;
    NTracing::TTraceContextPtr TraceContext_;

    THost* Host_;
    NLogging::TLogger Logger;
    DB::Block InputHeaderBlock_;
    DB::Block OutputHeaderBlock_;
    std::vector<int> IdToColumnIndex_;
    NTableClient::TRowBufferPtr RowBuffer_;
    DB::PrewhereInfoPtr PrewhereInfo_;

    DB::Block readImpl() override;
    void Prepare();
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TBlockInputStream> CreateBlockInputStream(
    NTableClient::ISchemalessMultiChunkReaderPtr reader,
    NTableClient::TTableSchema readSchema,
    NTracing::TTraceContextPtr traceContext,
    THost* host,
    NLogging::TLogger logger,
    DB::PrewhereInfoPtr prewhereInfo);

DB::BlockInputStreamPtr CreateBlockInputStreamLoggingAdapter(
    DB::BlockInputStreamPtr blockInputStream,
    NLogging::TLogger logger);

std::shared_ptr<TBlockInputStream> CreateBlockInputStream(
    TQueryContext* queryContext,
    const TSubquerySpec& subquerySpec,
    const DB::Names& columnNames,
    const NTracing::TTraceContextPtr& traceContext,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    DB::PrewhereInfoPtr prewhereInfo);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
