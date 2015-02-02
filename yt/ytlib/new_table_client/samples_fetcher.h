#pragma once

#include "public.h"
#include "unversioned_row.h"

#include <core/logging/log.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/fetcher_base.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NVersionedTableClient {

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
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        const NLog::TLogger& logger);

    virtual void AddChunk(NChunkClient::TRefCountedChunkSpecPtr chunk) override;
    virtual TFuture<void> Fetch() override;

    const std::vector<TOwningKey>& GetSamples() const;

private:
    TKeyColumns KeyColumns_;
    i64 DesiredSampleCount_;

    i64 SizeBetweenSamples_;
    i64 TotalDataSize_;

    //! All samples fetched so far.
    std::vector<TOwningKey> Samples_;

    virtual TFuture<void> FetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes) override;

    void DoFetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes);

};

DEFINE_REFCOUNTED_TYPE(TSamplesFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
