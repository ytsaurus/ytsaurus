#include "client_impl.h"
#include "encoded_row_stream.h"
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

#include <yt/yt/client/api/formatted_table_reader.h>
#include <yt/yt/client/api/table_partition_reader.h>

#include <yt/yt/client/formats/config.h>

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
    writerOptions->MemoryUsageTracker = HeavyRequestMemoryUsageTracker_;

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
        /*localHostName*/ std::string(), // Locality is not important during table upload.
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

    std::vector<i64> chunkCounts;
    chunkCounts.reserve(paths.size());
    i64 totalChunkCount = 0;

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
            .EnableReadSizeEstimation = options.EnableReadSizeEstimation,
        });

    for (const auto& path : paths) {
        YT_LOG_INFO("Collecting table input chunks (Path: %v)", path);

        if (!path.GetColumns().has_value()) {
            THROW_ERROR_EXCEPTION("Received ypath without column selectors")
                << TErrorAttribute("ypath", path);
        }

        auto transactionId = path.GetTransactionId();

        std::vector<i32> extensionTags;
        if (options.FetcherMode != EColumnarStatisticsFetcherMode::FromNodes) {
            extensionTags.push_back(TProtoExtensionTag<NTableClient::NProto::THeavyColumnStatisticsExt>::Value);
        }

        auto inputTableInfo = CollectInputTableInfo(
            path,
            this,
            nodeDirectory,
            options.FetchChunkSpecConfig,
            transactionId
                ? *transactionId
                : options.TransactionId,
            extensionTags,
            TGetUserObjectBasicAttributesOptions{},
            Logger);

        YT_VERIFY(!inputTableInfo.RlsReadSpec);

        chunkCounts.push_back(std::ssize(inputTableInfo.Chunks));
        totalChunkCount += std::ssize(inputTableInfo.Chunks);

        auto stableColumnNames = MapNamesToStableNames(*inputTableInfo.Schema, *path.GetColumns(), NonexistentColumnName);
        for (auto& inputChunk : inputTableInfo.Chunks) {
            fetcher->AddChunk(std::move(inputChunk), stableColumnNames, inputTableInfo.Schema);
        }
    }

    YT_LOG_INFO("Fetching columnar statistics (FetcherMode: %v, TotalChunkCount: %v)",
        options.FetcherMode,
        totalChunkCount);

    WaitFor(fetcher->Fetch())
        .ThrowOnError();

    const auto& chunkStatistics = fetcher->GetChunkStatistics();

    i64 statisticsIndex = 0;

    for (int pathIndex = 0; pathIndex < std::ssize(paths); ++pathIndex) {
        allStatistics.push_back(TColumnarStatistics::MakeEmpty(paths[pathIndex].GetColumns()->size()));
        for (i64 chunkIndex = 0; chunkIndex < chunkCounts[pathIndex]; ++statisticsIndex, ++chunkIndex) {
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

    auto chunkReaderHost = CreateSingleSourceMultiChunkReaderHost(TChunkReaderHost::FromClient(MakeStrong(this)));

    auto columnFilter = TColumnFilter{};
    auto tableReaderOptions = ToInternalTableReaderOptions(options);
    auto tableReaderConfig = options.Config ? options.Config : New<TTableReaderConfig>();
    TClientChunkReadOptions chunkReadOptions;

    bool isParallel = false;
    if (cookieProto.has_partition_mode()) {
        auto partitionMode = CheckedEnumCast<ETablePartitionMode>(cookieProto.partition_mode());
        switch (partitionMode) {
            case NTableClient::ETablePartitionMode::Sorted:
                THROW_ERROR_EXCEPTION("%Qlv partition mode is not supported by read_table_partition",
                    partitionMode);
            case NTableClient::ETablePartitionMode::Ordered:
                isParallel = false;
                break;
            case NTableClient::ETablePartitionMode::Unordered:
                isParallel = true;
                break;
        }
    }

    auto result = CreateMapJobReader(
        isParallel,
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
        schemas.push_back(dataSource->Schema());
        columnFilters.push_back(dataSource->Columns());
    }

    return MakeFuture(NApi::CreateTablePartitionReader(reader, schemas, columnFilters));
}

TFuture<IFormattedTableReaderPtr> TClient::CreateFormattedTableReader(
    const TRichYPath& path,
    const TYsonString& format,
    const TTableReaderOptions& options)
{
    return CreateTableReader(path, options).Apply(BIND([=] (const ITableReaderPtr& tableReader) {
        auto controlAttributesConfig = New<NFormats::TControlAttributesConfig>();
        controlAttributesConfig->EnableRowIndex = options.EnableRowIndex;
        controlAttributesConfig->EnableTableIndex = options.EnableTableIndex;
        controlAttributesConfig->EnableRangeIndex = options.EnableRangeIndex;

        return CreateEncodedRowStream(
            tableReader,
            tableReader->GetNameTable(),
            ConvertTo<NFormats::TFormat>(format),
            tableReader->GetTableSchema(),
            path.GetColumns(),
            std::move(controlAttributesConfig));
    }));
}

TFuture<IFormattedTableReaderPtr> TClient::CreateFormattedTablePartitionReader(
    const TTablePartitionCookiePtr& cookie,
    const TYsonString& format,
    const TReadTablePartitionOptions& options)
{
    return CreateTablePartitionReader(cookie, options).Apply(BIND([=] (const ITablePartitionReaderPtr& tableReader) {
        auto controlAttributesConfig = New<NFormats::TControlAttributesConfig>();
        controlAttributesConfig->EnableRowIndex = options.EnableRowIndex;
        controlAttributesConfig->EnableTableIndex = options.EnableTableIndex;
        controlAttributesConfig->EnableRangeIndex = options.EnableRangeIndex;

        auto schemas = NDetail::GetTableSchemas(tableReader);
        auto columnFilters = NDetail::GetColumnFilters(tableReader);

        return CreateEncodedRowStream(
            tableReader,
            tableReader->GetNameTable(),
            ConvertTo<NFormats::TFormat>(format),
            std::move(schemas[0]),
            std::move(columnFilters[0]),
            std::move(controlAttributesConfig));
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
