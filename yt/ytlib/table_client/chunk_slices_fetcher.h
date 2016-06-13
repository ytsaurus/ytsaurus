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
class TChunkSliceFetcher
    : public NChunkClient::TFetcherBase
{
public:
    TChunkSliceFetcher(
        NChunkClient::TFetcherConfigPtr config,
        i64 chunkSliceSize,
        const TKeyColumns& keyColumns,
        bool sliceByKeys,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        NChunkClient::TScrapeChunksCallback scraperCallback,
        NApi::INativeClientPtr client,
        const NLogging::TLogger& logger);

    virtual TFuture<void> Fetch() override;
    std::vector<NChunkClient::TInputSlicePtr> GetChunkSlices();

private:
    i64 ChunkSliceSize_;
    TKeyColumns KeyColumns_;
    bool SliceByKeys_;

    //! All slices fetched so far.
    std::vector<std::vector<NChunkClient::TInputSlicePtr>> SlicesByChunkIndex_;

    //! Number of slices in SlicesByChunkIndex_.
    i64 SliceCount_ = 0;


    virtual TFuture<void> FetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes) override;

    void DoFetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes);

};

DEFINE_REFCOUNTED_TYPE(TChunkSliceFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
