#pragma once

#include "public.h"
#include "unversioned_row.h"

#include <core/logging/log.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/fetcher_base.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TSample {
    TOwningKey Key;

    // Is true, if sample is trimmed to fulfil #MaxSampleSize_.
    bool Incomplete;
};

bool operator==(const TSample& lhs, const TSample& rhs);

bool operator<(const TSample& lhs, const TSample& rhs);

////////////////////////////////////////////////////////////////////////////////

//! Fetches samples for a bunch of table chunks by requesting
//! them directly from data nodes.
class TSamplesFetcher
    : public NChunkClient::TFetcherBase
{
public:
    TSamplesFetcher(
        NChunkClient::TFetcherConfigPtr config,
        i64 desiredSampleCount,
        const TKeyColumns& keyColumns,
        i32 maxSampleSize,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        NChunkClient::TScrapeChunksCallback scraperCallback,
        const NLogging::TLogger& logger);

    virtual void AddChunk(NChunkClient::TRefCountedChunkSpecPtr chunk) override;
    virtual TFuture<void> Fetch() override;

    const std::vector<TSample>& GetSamples() const;

private:
    const TKeyColumns KeyColumns_;
    const i64 DesiredSampleCount_;
    const i32 MaxSampleSize_;

    i64 SizeBetweenSamples_ = 0;
    i64 TotalDataSize_ = 0;

    //! All samples fetched so far.
    std::vector<TSample> Samples_;

    virtual TFuture<void> FetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes) override;

    void DoFetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes);

};

DEFINE_REFCOUNTED_TYPE(TSamplesFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
