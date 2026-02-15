#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

using TPerCategoryThrottlerProvider = TCallback<
    NConcurrency::IThroughputThrottlerPtr(EWorkloadCategory category)
>;

TPerCategoryThrottlerProvider MakeUniformPerCategoryThrottlerProvider(NConcurrency::IThroughputThrottlerPtr throttler);

using TPerClusterAndCategoryBandwidthThrottlerProvider = TCallback<
    TPerCategoryThrottlerProvider(const NScheduler::TClusterName& clusterName)
>;

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderHost
    : public TRefCounted
{
    TChunkReaderHost(
        NApi::NNative::IClientPtr client,
        NNodeTrackerClient::TNodeDescriptor localDescriptor,
        IBlockCachePtr blockCache,
        IClientChunkMetaCachePtr chunkMetaCache,
        TPerCategoryThrottlerProvider bandwidthThrottlerProvider,
        NConcurrency::IThroughputThrottlerPtr rpsThrottler,
        NConcurrency::IThroughputThrottlerPtr mediumThrottler,
        TTrafficMeterPtr trafficMeter);

    TChunkReaderHost(
        NApi::NNative::IClientPtr client,
        TPerCategoryThrottlerProvider bandwidthThrottlerProvider = {},
        NConcurrency::IThroughputThrottlerPtr rpsThrottler = nullptr,
        NConcurrency::IThroughputThrottlerPtr mediumThrottler = nullptr);

    const NApi::NNative::IClientPtr Client;

    const NNodeTrackerClient::TNodeDescriptor LocalDescriptor;

    const IBlockCachePtr BlockCache;
    const IClientChunkMetaCachePtr ChunkMetaCache;

    const TPerCategoryThrottlerProvider BandwidthThrottlerProvider;
    const NConcurrency::IThroughputThrottlerPtr RpsThrottler;
    const NConcurrency::IThroughputThrottlerPtr MediumThrottler;

    const TTrafficMeterPtr TrafficMeter;
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderHost)

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkReaderHost
    : public TRefCounted
{
public:
    struct TClusterContext
    {
        NScheduler::TClusterName Name;
        NApi::NNative::IClientPtr Client;
        TChunkReaderStatisticsPtr ChunkReaderStatistics;
    };

    TMultiChunkReaderHost(
        TChunkReaderHostPtr baseHost,
        TPerClusterAndCategoryBandwidthThrottlerProvider bandwidthThrottlerProvider,
        std::vector<TClusterContext> clusterContextList);

    explicit TMultiChunkReaderHost(
        TChunkReaderHostPtr baseHost);

    TChunkReaderHostPtr CreateHostForCluster(const NScheduler::TClusterName& clusterName);

    TClientChunkReadOptions AdjustClientChunkReadOptions(
        const NScheduler::TClusterName& clusterName,
        const TClientChunkReadOptions& options) const;

    const TTrafficMeterPtr& GetTrafficMeter() const;

    const THashMap<NScheduler::TClusterName, TChunkReaderStatisticsPtr>& GetChunkReaderStatistics() const;

private:
    const TChunkReaderHostPtr BaseHost_;
    const TPerClusterAndCategoryBandwidthThrottlerProvider BandwidthThrottlerProvider_;

    const THashMap<NScheduler::TClusterName, TChunkReaderHostPtr> Hosts_;
    const THashMap<NScheduler::TClusterName, TChunkReaderStatisticsPtr> ChunkReaderStatisticsMap_;
};

DEFINE_REFCOUNTED_TYPE(TMultiChunkReaderHost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
