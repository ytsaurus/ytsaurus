#pragma once

#include "private.h"

#include "columnar_statistics.h"

#include <yt/ytlib/chunk_client/fetcher.h>

#include <yt/ytlib/chunk_client/data_node_service_proxy.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Fetches columnary statistics for a bunch of table chunks by requesting
//! them directly from data nodes.
class TColumnarStatisticsFetcher
    : public NChunkClient::TFetcherBase
{
public:
    TColumnarStatisticsFetcher(
        NChunkClient::TFetcherConfigPtr config,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        NChunkClient::IFetcherChunkScraperPtr chunkScraper,
        NApi::INativeClientPtr client,
        const NLogging::TLogger& logger,
        std::vector<TString> columnNames);

    // Return per-column total data weight across all requested chunks.
    TColumnarStatistics GetColumnarStatistics() const;

    TFuture<void> Fetch() override;

private:
    const std::vector<TString> ColumnNames_;
    std::vector<TColumnarStatistics> ChunkStatistics_;

    virtual TFuture<void> FetchFromNode(NNodeTrackerClient::TNodeId nodeId, std::vector<int> chunkIndexes) override;

    TFuture<void> DoFetchFromNode(NNodeTrackerClient::TNodeId nodeId, std::vector<int> chunkIndexes);

    void OnResponse(
        NNodeTrackerClient::TNodeId nodeId,
        const std::vector<int>& chunkIndexes,
        const NChunkClient::TDataNodeServiceProxy::TErrorOrRspGetColumnarStatisticsPtr& rspOrError);

    TColumnarStatistics GetEmptyStatistics() const;
};

DEFINE_REFCOUNTED_TYPE(TColumnarStatisticsFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
