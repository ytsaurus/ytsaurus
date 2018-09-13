#include "chunk_reader.h"

#include "private.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/config.h>

#include <yt/client/api/table_reader.h>
#include <yt/client/node_tracker_client/node_directory.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/schemaful_reader_adapter.h>

#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NClickHouse {

using namespace NYT::NApi;
using namespace NYT::NChunkClient;
using namespace NYT::NConcurrency;
using namespace NYT::NNodeTrackerClient;
using namespace NYT::NTableClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

void VerifyDataSourcesAreHomogeneous(const TDataSourceDirectoryPtr dataSourceDirectory)
{
    auto& dataSources = dataSourceDirectory->DataSources();

    if (dataSources.size() <= 1) {
        return;
    }

    auto& representative = dataSources.front();
    for (size_t i = 1; i < dataSources.size(); ++i) {
        auto& current = dataSources[i];
        if (current.GetType() != representative.GetType()) {
            THROW_ERROR_EXCEPTION("Cannot mix static and dynamic tables");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TTableSchema AddSystemColumns(
    const TTableSchema& schema,
    const NTableClient::TTableReaderOptionsPtr& options)
{
    auto columns = schema.Columns();

    if (options->EnableTableIndex) {
        columns.emplace_back(TableIndexColumnName, EValueType::Int64);
    }

    return TTableSchema(columns, schema.GetStrict(), schema.GetUniqueKeys());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateChunkReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NNative::IClientPtr client,
    TNodeDirectoryPtr nodeDirectory,
    TDataSourceDirectoryPtr dataSourceDirectory,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    IThroughputThrottlerPtr bandwidthThrottler,
    const TTableSchema& readerSchema,
    bool allowUnorderedRead)
{
    YCHECK(!dataSourceDirectory->DataSources().empty());

    VerifyDataSourcesAreHomogeneous(dataSourceDirectory);
    const auto& representativeDataSource = dataSourceDirectory->DataSources().front();
    const auto dataSourceType = representativeDataSource.GetType();

    auto factory = [=] (
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter) -> ISchemalessReaderPtr
    {
        if (dataSourceType == EDataSourceType::UnversionedTable) {
            auto createReader = allowUnorderedRead
                ? CreateSchemalessParallelMultiReader
                : CreateSchemalessSequentialMultiReader;

            // TODO(max42): fill properly.
            TClientBlockReadOptions blockReadOptions;
            blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
            blockReadOptions.WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserRealtime);

            return createReader(
                config,
                options,
                client,
                TNodeDescriptor(),
                client->GetNativeConnection()->GetBlockCache(),
                nodeDirectory,
                dataSourceDirectory,
                dataSliceDescriptors,
                nameTable,
                blockReadOptions,
                columnFilter,
                TKeyColumns(),
                Null,
                nullptr /* trafficMeter */,
                bandwidthThrottler,
                GetUnlimitedThrottler() /* rps throttler */);
        } else if (dataSourceType == EDataSourceType::VersionedTable) {
            YCHECK(dataSliceDescriptors.size() == 1);

            // TODO(max42): fill properly.
            TClientBlockReadOptions blockReadOptions;
            blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
            blockReadOptions.WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserRealtime);

            return CreateSchemalessMergingMultiChunkReader(
                config,
                options,
                client,
                TNodeDescriptor(),
                client->GetNativeConnection()->GetBlockCache(),
                nodeDirectory,
                dataSourceDirectory,
                dataSliceDescriptors[0],
                nameTable,
                blockReadOptions,
                columnFilter,
                nullptr /* trafficMeter */,
                bandwidthThrottler,
                GetUnlimitedThrottler() /* rps throttler */);
        } else {
            THROW_ERROR_EXCEPTION(
                "Invalid job specification: unsupported data source type %Qv", dataSourceType);
        }
    };

    return CreateSchemafulReaderAdapter(
        std::move(factory),
        AddSystemColumns(readerSchema, options));
}

void WarmUp(std::vector<NTableClient::ISchemafulReaderPtr>& chunkReaders)
{
    for (const auto& reader : chunkReaders) {
        WaitFor(reader->GetReadyEvent())
            .ThrowOnError();
    }
}

}   // namespace NClickHouse
}   // namespace NYT
