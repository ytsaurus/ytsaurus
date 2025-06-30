#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderHost
    : public TRefCounted
{
    TChunkReaderHost(
        NApi::NNative::IClientPtr client,
        NNodeTrackerClient::TNodeDescriptor localDescriptor,
        IBlockCachePtr blockCache,
        IClientChunkMetaCachePtr chunkMetaCache,
        NNodeTrackerClient::INodeStatusDirectoryPtr nodeStatusDirectory,
        NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
        NConcurrency::IThroughputThrottlerPtr rpsThrottler,
        NConcurrency::IThroughputThrottlerPtr mediumThrottler,
        TTrafficMeterPtr trafficMeter);

    const NApi::NNative::IClientPtr Client;

    const NNodeTrackerClient::TNodeDescriptor LocalDescriptor;

    const IBlockCachePtr BlockCache;
    const IClientChunkMetaCachePtr ChunkMetaCache;

    const NNodeTrackerClient::INodeStatusDirectoryPtr NodeStatusDirectory;

    const NConcurrency::IThroughputThrottlerPtr BandwidthThrottler;
    const NConcurrency::IThroughputThrottlerPtr RpsThrottler;
    const NConcurrency::IThroughputThrottlerPtr MediumThrottler;

    const TTrafficMeterPtr TrafficMeter;

    static TChunkReaderHostPtr FromClient(
        NApi::NNative::IClientPtr client,
        NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
        NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler(),
        NConcurrency::IThroughputThrottlerPtr mediumThrottler = NConcurrency::GetUnlimitedThrottler());
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderHost)

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkReaderHost
    : public TRefCounted
{
public:
    using TBandwidthThrottlerFactory = TCallback<NConcurrency::IThroughputThrottlerPtr(const NScheduler::TClusterName& clusterName)>;

    struct TClusterContext
    {
        NScheduler::TClusterName Name;
        NApi::NNative::IClientPtr Client;
        TChunkReaderStatisticsPtr ChunkReaderStatistics;
    };

    TMultiChunkReaderHost(
        TChunkReaderHostPtr baseHost,
        TBandwidthThrottlerFactory bandwidthThrottlerFactory,
        std::vector<TClusterContext> clusterContextList);

    TChunkReaderHostPtr CreateHostForCluster(const NScheduler::TClusterName& clusterName);
    TClientChunkReadOptions AdjustClientChunkReadOptions(
        const NScheduler::TClusterName& clusterName,
        const TClientChunkReadOptions& options) const;
    TTrafficMeterPtr GetTrafficMeter() const;

    const THashMap<NScheduler::TClusterName, TChunkReaderStatisticsPtr>& GetChunkReaderStatistics() const;

private:
    const TChunkReaderHostPtr BaseHost_;
    const TBandwidthThrottlerFactory BandwidthThrottlerFactory_;

    const THashMap<NScheduler::TClusterName, TChunkReaderHostPtr> Hosts_;
    const THashMap<NScheduler::TClusterName, TChunkReaderStatisticsPtr> ChunkReaderStatisticsMap_;
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkReaderHost)

TMultiChunkReaderHostPtr CreateMultiChunkReaderHost(
    TChunkReaderHostPtr baseHost,
    TMultiChunkReaderHost::TBandwidthThrottlerFactory bandwidthThrottlerFactory,
    std::vector<TMultiChunkReaderHost::TClusterContext> clusterContextList);

TMultiChunkReaderHostPtr CreateSingleSourceMultiChunkReaderHost(
    TChunkReaderHostPtr baseHost);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
