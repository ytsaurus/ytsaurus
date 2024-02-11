#include "client_impl.h"
#include "table_reader.h"
#include "table_writer.h"
#include "partition_tables.h"
#include "skynet.h"

#include <yt/yt/ytlib/node_tracker_client/channel.h>
#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/columnar_statistics_fetcher.h>
#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/guid.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NApi::NNative {

using namespace NYPath;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TFuture<ITableReaderPtr> TClient::CreateTableReader(
    const TRichYPath& path,
    const TTableReaderOptions& options)
{
    return NNative::CreateTableReader(this, path, options, New<TNameTable>());
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
    return NNative::CreateTableWriter(this, path, options);
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
        // TODO(galtsev): OperationId
        Logger.WithTag("Name: Root").WithTag("OperationId: %v", NJobTrackerClient::NullOperationId));

    return partitioner.PartitionTables();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
