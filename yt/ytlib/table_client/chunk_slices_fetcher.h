#pragma once

#include "public.h"

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/fetcher_base.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Fetches samples for a bunch of table chunks by requesting
//! them directly from data nodes.
class TChunkSlicesFetcher
    : public NChunkClient::TFetcherBase
{
public:
    TChunkSlicesFetcher(
        NChunkClient::TFetcherConfigPtr config,
        i64 chunkSliceSize,
        const TKeyColumns& keyColumns,
        bool sliceByKeys,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        NChunkClient::TScrapeChunksCallback scraperCallback,
        const NLogging::TLogger& logger);


    virtual TFuture<void> Fetch() override;
    const std::vector<NChunkClient::TChunkSlicePtr>& GetChunkSlices() const;

private:
    i64 ChunkSliceSize_;
    TKeyColumns KeyColumns_;
    bool SliceByKeys_;

    //! All samples fetched so far.
    std::vector<NChunkClient::TChunkSlicePtr> ChunkSlices_;


    virtual TFuture<void> FetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes) override;

    void DoFetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes);

};

DEFINE_REFCOUNTED_TYPE(TChunkSlicesFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
