#pragma once

#include "public.h"
#include "unversioned_row.h"

#include <yt/ytlib/chunk_client/fetcher.h>
#include <yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESamplingPolicy,
    ((Sorting)             (0))
    ((Partitioning)        (1))
);

////////////////////////////////////////////////////////////////////////////////

struct TSample
{
    //! The key is stored in row buffer.
    TKey Key;

    //! |true| if the sample is trimmed to obey max sample size limit.
    bool Incomplete;

    //! Proportional to the data size this sample represents.
    i64 Weight;
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
        ESamplingPolicy samplingPolicy,
        int desiredSampleCount,
        const TKeyColumns& keyColumns,
        i64 maxSampleSize,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        NTableClient::TRowBufferPtr rowBuffer,
        NChunkClient::IFetcherChunkScraperPtr chunkScraper,
        NApi::INativeClientPtr client,
        const NLogging::TLogger& logger);

    virtual void AddChunk(NChunkClient::TInputChunkPtr chunk) override;
    virtual TFuture<void> Fetch() override;

    const std::vector<TSample>& GetSamples() const;

private:
    const ESamplingPolicy SamplingPolicy_;
    const TKeyColumns KeyColumns_;
    const int DesiredSampleCount_;
    const i64 MaxSampleSize_;

    i64 SizeBetweenSamples_ = 0;
    i64 TotalDataSize_ = 0;

    //! All samples fetched so far.
    std::vector<TSample> Samples_;

    virtual TFuture<void> FetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes) override;

    TFuture<void> DoFetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        const std::vector<int>& chunkIndexes);

    void OnResponse(
        NNodeTrackerClient::TNodeId nodeId,
        const std::vector<int>& requestedChunkIndexes,
        const NChunkClient::TDataNodeServiceProxy::TErrorOrRspGetTableSamplesPtr& rspOrError);

};

DEFINE_REFCOUNTED_TYPE(TSamplesFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
