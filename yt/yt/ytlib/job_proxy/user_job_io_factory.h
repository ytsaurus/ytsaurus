#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/data_sink.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct IUserJobWriterFactory
    : public virtual TRefCounted
{
    virtual NTableClient::ISchemalessMultiChunkWriterPtr CreateWriter(
        NApi::NNative::IClientPtr client,
        NTableClient::TTableWriterConfigPtr config,
        NTableClient::TTableWriterOptionsPtr options,
        NChunkClient::TChunkListId chunkListId,
        NTransactionClient::TTransactionId transactionId,
        NTableClient::TTableSchemaPtr tableSchema,
        NTableClient::TMasterTableSchemaId schemaId,
        const NTableClient::TChunkTimestamps& chunkTimestamps,
        const std::optional<NChunkClient::TDataSink>& dataSink,
        NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions) = 0;
};

DEFINE_REFCOUNTED_TYPE(IUserJobWriterFactory)

IUserJobWriterFactoryPtr CreateUserJobWriterFactory(
    const IJobSpecHelperPtr& jobSpecHelper,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    TString localHostName,
    NConcurrency::IThroughputThrottlerPtr outBandwidthThrottler);

////////////////////////////////////////////////////////////////////////////////

struct TCreateUserJobReaderResult
{
    NTableClient::ISchemalessMultiChunkReaderPtr Reader;
    std::optional<NChunkClient::NProto::TDataStatistics> PreparationDataStatistics;
};

TCreateUserJobReaderResult CreateUserJobReader(
    const IJobSpecHelperPtr& jobSpecHelper,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    TClosure onNetworkReleased,
    NTableClient::TNameTablePtr nameTable,
    const NTableClient::TColumnFilter& columnFilter);

TCreateUserJobReaderResult CreateMapJobReader(
    bool isParallel,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory,
    const NTableClient::TTableReaderOptionsPtr& tableReaderOptions,
    const NTableClient::TTableReaderConfigPtr& tableReaderConfig,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    NTableClient::TNameTablePtr nameTable,
    const NTableClient::TColumnFilter& columnFilter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
