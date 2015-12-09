#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/fetcher_base.h>
#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Fetches slices for a bunch of table chunks by requesting
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
    std::vector<NChunkClient::TChunkSlicePtr> GetChunkSlices();

private:
    i64 ChunkSliceSize_;
    TKeyColumns KeyColumns_;
    bool SliceByKeys_;

    //! All slices fetched so far.
    std::vector<std::vector<NChunkClient::TChunkSlicePtr>> SlicesByChunkIndex_;

    //! Number of slices in SlicesByChunkIndex_.
    i64 SliceCount_ = 0;


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
