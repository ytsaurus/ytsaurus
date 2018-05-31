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
    using TFetcherBase::TFetcherBase;

    //! Return per-chunk columnar statistics.
    const std::vector<TColumnarStatistics>& GetChunkStatistics() const;
    //! Set column selectivity factor for all processed chunks according to the fetched columnar statistics.
    void SetColumnSelectivityFactors() const;

    TFuture<void> Fetch() override;

    void AddChunk(NChunkClient::TInputChunkPtr chunk, std::vector<TString> columnNames);

private:
    std::vector<TColumnarStatistics> ChunkStatistics_;
    std::vector<std::vector<TString>> ChunkColumnNames_;

    // Columnar statistics fetcher does not support adding pure chunks as each chunk should be provided with
    // a column list to fetch statistics for.
    using TFetcherBase::AddChunk;

    virtual TFuture<void> FetchFromNode(NNodeTrackerClient::TNodeId nodeId, std::vector<int> chunkIndexes) override;

    TFuture<void> DoFetchFromNode(NNodeTrackerClient::TNodeId nodeId, std::vector<int> chunkIndexes);

    void OnResponse(
        NNodeTrackerClient::TNodeId nodeId,
        const std::vector<int>& chunkIndexes,
        const NChunkClient::TDataNodeServiceProxy::TErrorOrRspGetColumnarStatisticsPtr& rspOrError);
};

DEFINE_REFCOUNTED_TYPE(TColumnarStatisticsFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
