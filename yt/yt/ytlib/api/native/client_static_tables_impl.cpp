#include "client_impl.h"
#include "table_reader.h"
#include "partition_tables.h"
#include "skynet.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/yt/ytlib/job_proxy/helpers.h>
#include <yt/yt/ytlib/job_proxy/user_job_io_factory.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>
#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/columnar_statistics_fetcher.h>
#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/ytlib/table_client/proto/table_partition_cookie.pb.h>

#include <yt/yt/client/api/table_partition_reader.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/guid.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NApi::NNative {

using namespace NYPath;
using namespace NYson;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NConcurrency;
using namespace NJobProxy;

////////////////////////////////////////////////////////////////////////////////

TFuture<ITableReaderPtr> TClient::CreateTableReader(
    const TRichYPath& path,
    const TTableReaderOptions& options)
{
    return NNative::CreateTableReader(
        this,
        path,
        options,
        New<TNameTable>(),
        /*columnFilter*/ {},
        /*bandwidthThrottler*/ GetUnlimitedThrottler(),
        /*rpsThrottler*/ GetUnlimitedThrottler(),
        HeavyRequestMemoryUsageTracker_);
}

TFuture<TSkynetSharePartsLocationsPtr> TClient::LocateSkynetShare(
    const TRichYPath& path,
    const TLocateSkynetShareOptions& options)
{
    return NNative::LocateSkynetShare(this, path, options);
}

TFuture<ITableWriterPtr> TClient::CreateTableWriter(
    const NYPath::TRichYPath& path,
    const TTableWriterOptions& options)
{
    auto nameTable = New<TNameTable>();
    nameTable->SetEnableColumnNameValidation();

    auto writerOptions = New<NTableClient::TTableWriterOptions>();
    writerOptions->EnableValidationOptions(/*validateAnyIsValidYson*/ options.ValidateAnyIsValidYson);

    NApi::ITransactionPtr transaction;
    if (options.TransactionId) {
        TTransactionAttachOptions transactionOptions;
        transactionOptions.Ping = options.Ping;
        transactionOptions.PingAncestors = options.PingAncestors;
        transaction = AttachTransaction(options.TransactionId, transactionOptions);
    }

    auto asyncSchemalessWriter = CreateSchemalessTableWriter(
        options.Config ? options.Config : New<TTableWriterConfig>(),
        writerOptions,
        path,
        nameTable,
        this,
        /*localHostName*/ TString(), // Locality is not important during table upload.
        transaction,
        /*writeBlocksOptions*/ {});

    return asyncSchemalessWriter.Apply(BIND([] (const IUnversionedWriterPtr& schemalessWriter) {
        return CreateApiFromSchemalessWriterAdapter(std::move(schemalessWriter));
    }));
}

std::vector<TColumnarStatistics> TClient::DoGetColumnarStatistics(
    const std::vector<TRichYPath>& paths,
    const TGetColumnarStatisticsOptions& options)
{
    std::vector<TColumnarStatistics> allStatistics;
    allStatistics.reserve(paths.size());

    std::vector<ui64> chunkCount;
    chunkCount.reserve(paths.size());

    auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    auto fetcher = New<TColumnarStatisticsFetcher>(
        CreateSerializedInvoker(GetCurrentInvoker()),
        this /*client*/,
        TColumnarStatisticsFetcher::TOptions{
            .Config = options.FetcherConfig,
            .NodeDirectory = nodeDirectory,
            .Mode = options.FetcherMode,
            .StoreChunkStatistics = true,
            .EnableEarlyFinish = options.EnableEarlyFinish,
            .Logger = Logger,
        });

    for (const auto& path : paths) {
        YT_LOG_INFO("Collecting table input chunks (Path: %v)", path);

        auto transactionId = path.GetTransactionId();

        std::vector<i32> extensionTags;
        if (options.FetcherMode != EColumnarStatisticsFetcherMode::FromNodes) {
            extensionTags.push_back(TProtoExtensionTag<NTableClient::NProto::THeavyColumnStatisticsExt>::Value);
        }

        auto [inputChunks, schema, _] = CollectTableInputChunks(
            path,
            this,
            nodeDirectory,
            options.FetchChunkSpecConfig,
            transactionId
                ? *transactionId
                : options.TransactionId,
            extensionTags,
            Logger);

        YT_LOG_INFO("Fetching columnar statistics (Columns: %v, FetcherMode: %v)",
            *path.GetColumns(),
            options.FetcherMode);

        YT_VERIFY(path.GetColumns().operator bool());
        auto stableColumnNames = MapNamesToStableNames(*schema, *path.GetColumns(), NonexistentColumnName);
        for (const auto& inputChunk : inputChunks) {
            fetcher->AddChunk(inputChunk, stableColumnNames);
        }
        chunkCount.push_back(inputChunks.size());
    }

    WaitFor(fetcher->Fetch())
        .ThrowOnError();

    const auto& chunkStatistics = fetcher->GetChunkStatistics();

    ui64 statisticsIndex = 0;

    for (int pathIndex = 0; pathIndex < std::ssize(paths); ++pathIndex) {
        allStatistics.push_back(TColumnarStatistics::MakeEmpty(paths[pathIndex].GetColumns()->size()));
        for (ui64 chunkIndex = 0; chunkIndex < chunkCount[pathIndex]; ++statisticsIndex, ++chunkIndex) {
            allStatistics[pathIndex] += chunkStatistics[statisticsIndex];
        }
    }
    return allStatistics;
}

////////////////////////////////////////////////////////////////////////////////

TMultiTablePartitions TClient::DoPartitionTables(
    const std::vector<TRichYPath>& paths,
    const TPartitionTablesOptions& options)
{
    TMultiTablePartitioner partitioner(
        /*client*/ this,
        paths,
        options,
        Options_.GetAuthenticatedUser(),
        // TODO(galtsev): OperationId
        Logger().WithTag("Name: Root").WithTag("OperationId: %v", NJobTrackerClient::NullOperationId));

    return partitioner.PartitionTables();
}

TFuture<ITablePartitionReaderPtr> TClient::CreateTablePartitionReader(
    const TTablePartitionCookiePtr& cookie,
    const TReadTablePartitionOptions& options)
{
    NTableClient::NProto::TTablePartitionCookie cookieProto;
    if (!cookieProto.ParseFromString(cookie.Underlying()->Payload())) {
        THROW_ERROR_EXCEPTION("Failed to parse table partition cookie");
    }

    if (cookieProto.user() != Options_.GetAuthenticatedUser()) {
        THROW_ERROR_EXCEPTION("Partition must be read by the same user who created it")
            << TErrorAttribute("read_partition_user", Options_.GetAuthenticatedUser())
            << TErrorAttribute("partition_tables_user", cookieProto.user());
    }

    auto dataSliceDescriptors = UnpackDataSliceDescriptors(cookieProto.table_input_specs());
    auto dataSourceDirectory = FromProto<NChunkClient::TDataSourceDirectoryPtr>(cookieProto.data_source_directory());

    auto nameTable = New<TNameTable>();

    auto chunkReaderHost = TChunkReaderHost::FromClient(MakeStrong(this));

    auto columnFilter = TColumnFilter{};
    auto tableReaderOptions = ToInternalTableReaderOptions(options);
    auto tableReaderConfig = options.Config ? options.Config : New<TTableReaderConfig>();
    TClientChunkReadOptions chunkReadOptions;

    auto result = CreateMapJobReader(
        /*isParallel*/ true,
        dataSliceDescriptors,
        dataSourceDirectory,
        tableReaderOptions,
        tableReaderConfig,
        chunkReadOptions,
        chunkReaderHost,
        nameTable,
        columnFilter);
    auto reader = ToApiRowBatchReader(result.Reader);

    std::vector<TTableSchemaPtr> schemas;
    std::vector<TColumnNameFilter> columnFilters;
    for (const auto& dataSource : dataSourceDirectory->DataSources()) {
        schemas.push_back(dataSource.Schema());
        columnFilters.push_back(dataSource.Columns());
    }

    return MakeFuture(NApi::CreateTablePartitionReader(reader, schemas, columnFilters));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
