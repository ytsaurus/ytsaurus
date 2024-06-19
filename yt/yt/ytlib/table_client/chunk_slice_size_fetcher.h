#pragma once

#include "public.h"
#include "column_filter_dictionary.h"

#include <yt/yt/ytlib/chunk_client/fetcher.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkSliceSizeFetcher
    : public NChunkClient::TFetcherBase
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<NChunkClient::TWeightedInputChunkPtr>, WeightedChunks);

public:
    TChunkSliceSizeFetcher(
        NChunkClient::TFetcherConfigPtr config,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        NChunkClient::IFetcherChunkScraperPtr chunkScraper,
        NApi::NNative::IClientPtr client,
        const NLogging::TLogger& logger);

    // TODO(achulkov2): Generalize column-related logic into the main interface and TFetcherBase.
    // It is similar in TColumnarStatisticsFetcher and the same will eventually be needed in ChunkSliceFetcher.

    //! A list of columns can be specified to be used as a filter.
    //! Only blocks containing at least one of the specified columns will be considered.
    void AddChunk(NChunkClient::TInputChunkPtr chunk, std::vector<TColumnStableName> columnStableNames);
    //! NB: When not provided, no column filter is assumed.
    using TFetcherBase::AddChunk;

    //! Set block selectivity factor for all processed chunks according to the fetched data weights.
    void ApplyBlockSelectivityFactors() const;

private:
    THashMap<int, int> ChunkColumnFilterIds_;
    TColumnStableNameFilterDictionary ColumnFilterDictionary_;

    const std::optional<std::vector<TColumnStableName>> GetColumnStableNames(int chunkIndex) const;

    void ProcessDynamicStore(int chunkIndex) override;

    TFuture<void> FetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes) override;

    TFuture<void> DoFetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes);

    void OnResponse(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> requestedChunkIndexes,
        const NChunkClient::TDataNodeServiceProxy::TErrorOrRspGetChunkSliceDataWeightsPtr& rspOrError);

    void OnFetchingStarted() override;
};

DEFINE_REFCOUNTED_TYPE(TChunkSliceSizeFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
