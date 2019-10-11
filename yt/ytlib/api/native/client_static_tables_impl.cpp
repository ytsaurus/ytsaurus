#include "client_impl.h"
#include "table_reader.h"
#include "table_writer.h"
#include "skynet.h"

#include <yt/ytlib/table_client/columnar_statistics_fetcher.h>
#include <yt/ytlib/table_client/helpers.h>

#include <yt/client/table_client/name_table.h>

#include <yt/core/concurrency/action_queue.h>

namespace NYT::NApi::NNative {

using namespace NYPath;
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
        options.FetcherConfig,
        nodeDirectory,
        CreateSerializedInvoker(GetCurrentInvoker()),
        nullptr /* scraper */,
        this,
        Logger);

    for (const auto& path : paths) {
        YT_LOG_INFO("Collecting table input chunks (Path: %v)", path);

        auto transactionId = path.GetTransactionId();

        auto inputChunks = CollectTableInputChunks(
            path,
            this,
            nodeDirectory,
            options.FetchChunkSpecConfig,
            transactionId
                ? *transactionId
                : options.TransactionId,
            Logger);

        YT_LOG_INFO("Fetching columnar statistics (Columns: %v)", *path.GetColumns());


        for (const auto& inputChunk : inputChunks) {
            fetcher->AddChunk(inputChunk, *path.GetColumns());
        }
        chunkCount.push_back(inputChunks.size());
    }

    WaitFor(fetcher->Fetch())
        .ThrowOnError();

    const auto& chunkStatistics = fetcher->GetChunkStatistics();

    ui64 statisticsIndex = 0;

    for (int pathIndex = 0; pathIndex < paths.size(); ++pathIndex) {
        allStatistics.push_back(TColumnarStatistics::MakeEmpty(paths[pathIndex].GetColumns()->size()));
        for (ui64 chunkIndex = 0; chunkIndex < chunkCount[pathIndex]; ++statisticsIndex, ++chunkIndex) {
            allStatistics[pathIndex] += chunkStatistics[statisticsIndex];
        }
    }
    return allStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
