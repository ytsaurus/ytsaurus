#include "chunk_reader_host.h"
#include "chunk_reader_options.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

#include <yt/yt/ytlib/node_tracker_client/node_status_directory.h>

#include <yt/yt/ytlib/chunk_client/block_cache.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NApi;
using namespace NScheduler;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

THashMap<TClusterName, TChunkReaderHostPtr> CreateHostMap(
    const TChunkReaderHostPtr& baseHost,
    const std::vector<TMultiChunkReaderHost::TClusterContext>& clusterContextList)
{
    THashMap<TClusterName, TChunkReaderHostPtr> hosts;
    for (const auto& clusterOptions : clusterContextList) {
        hosts.emplace(
            clusterOptions.Name,
            New<TChunkReaderHost>(
                clusterOptions.Client,
                baseHost->LocalDescriptor,
                baseHost->BlockCache,
                baseHost->ChunkMetaCache,
                baseHost->NodeStatusDirectory,
                baseHost->BandwidthThrottlerProvider,
                baseHost->RpsThrottler,
                baseHost->MediumThrottler,
                baseHost->TrafficMeter));
    }
    return hosts;
}

THashMap<TClusterName, TChunkReaderStatisticsPtr> CreateChunkReaderStatistics(
    const std::vector<TMultiChunkReaderHost::TClusterContext>& clusterContextList)
{
    THashMap<TClusterName, TChunkReaderStatisticsPtr> chunkReaderStatistics;
    for (auto& clusterOptions : clusterContextList) {
        chunkReaderStatistics.emplace(
            clusterOptions.Name,
            clusterOptions.ChunkReaderStatistics);
    }
    return chunkReaderStatistics;
}

const TPerCategoryThrottlerProvider& GetUnlimitedPerCategoryThrottlerProvider()
{
    static const auto UnlimitedThrottlerProvider = BIND([] (EWorkloadCategory /*category*/) -> IThroughputThrottlerPtr {
        return GetUnlimitedThrottler();
    });
    return UnlimitedThrottlerProvider;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TPerCategoryThrottlerProvider MakeUniformPerCategoryThrottlerProvider(IThroughputThrottlerPtr throttler)
{
    return BIND([throttler = std::move(throttler)] (EWorkloadCategory /*category*/) {
        return throttler;
    });
}

////////////////////////////////////////////////////////////////////////////////

TChunkReaderHost::TChunkReaderHost(
    NNative::IClientPtr client,
    TNodeDescriptor localDescriptor,
    IBlockCachePtr blockCache,
    IClientChunkMetaCachePtr chunkMetaCache,
    INodeStatusDirectoryPtr nodeStatusDirectory,
    TPerCategoryThrottlerProvider bandwidthThrottlerProvider,
    IThroughputThrottlerPtr rpsThrottler,
    IThroughputThrottlerPtr mediumThrottler,
    TTrafficMeterPtr trafficMeter)
    : Client(std::move(client))
    , LocalDescriptor(std::move(localDescriptor))
    , BlockCache(blockCache ? std::move(blockCache) : GetNullBlockCache())
    // Could be null.
    , ChunkMetaCache(std::move(chunkMetaCache))
    , NodeStatusDirectory(nodeStatusDirectory ? std::move(nodeStatusDirectory) : CreateTrivialNodeStatusDirectory())
    , BandwidthThrottlerProvider(bandwidthThrottlerProvider ? std::move(bandwidthThrottlerProvider) : GetUnlimitedPerCategoryThrottlerProvider())
    , RpsThrottler(rpsThrottler ? std::move(rpsThrottler) : GetUnlimitedThrottler())
    , MediumThrottler(mediumThrottler ? std::move(mediumThrottler) : GetUnlimitedThrottler())
    // Could be null.
    , TrafficMeter(std::move(trafficMeter))
{
    YT_VERIFY(Client);
}

TChunkReaderHost::TChunkReaderHost(
    NNative::IClientPtr client,
    TPerCategoryThrottlerProvider bandwidthThrottlerProvider,
    IThroughputThrottlerPtr rpsThrottler,
    IThroughputThrottlerPtr mediumThrottler)
    : TChunkReaderHost(
        client,
        /*localDescriptor*/ TNodeDescriptor{},
        client->GetNativeConnection()->GetBlockCache(),
        client->GetNativeConnection()->GetChunkMetaCache(),
        /*nodeStatusDirectory*/ nullptr,
        std::move(bandwidthThrottlerProvider),
        std::move(rpsThrottler),
        std::move(mediumThrottler),
        /*trafficMeter*/ nullptr)
{ }

////////////////////////////////////////////////////////////////////////////////

TMultiChunkReaderHost::TMultiChunkReaderHost(
    TChunkReaderHostPtr baseHost,
    TPerClusterAndCategoryBandwidthThrottlerProvider bandwidthThrottlerFactory,
    std::vector<TClusterContext> clusterContextList)
    : BaseHost_(std::move(baseHost))
    , BandwidthThrottlerProvider_(std::move(bandwidthThrottlerFactory))
    , Hosts_(CreateHostMap(BaseHost_, clusterContextList))
    , ChunkReaderStatisticsMap_(CreateChunkReaderStatistics(clusterContextList))
{
    YT_VERIFY(AnyOf(clusterContextList, [] (const auto& clusterOptions) { return IsLocal(clusterOptions.Name); }));
}

TMultiChunkReaderHost::TMultiChunkReaderHost(
    TChunkReaderHostPtr baseHost)
    : TMultiChunkReaderHost(
        baseHost,
        /*bandwidthThrottlerProvider*/ {},
        std::vector{
            TMultiChunkReaderHost::TClusterContext{
                .Name = LocalClusterName,
                .Client = baseHost->Client,
            },
        })
{ }

TChunkReaderHostPtr TMultiChunkReaderHost::CreateHostForCluster(
    const NScheduler::TClusterName& clusterName)
{
    auto host = GetOrCrash(Hosts_, clusterName);

    return New<TChunkReaderHost>(
        host->Client,
        host->LocalDescriptor,
        host->BlockCache,
        host->ChunkMetaCache,
        host->NodeStatusDirectory,
        // NB(coteeq, yuryalekseev): All this thing with factory is here because of
        // cross-cluster throttlers in exe-node. A new throttler could be configured
        // while job is already running and we would like to throttle the job
        // in this case. Obviously, this scheme will not throttle already-running
        // readers, but it's a good enough approximation.
        BandwidthThrottlerProvider_
            ? BandwidthThrottlerProvider_(clusterName)
            : host->BandwidthThrottlerProvider,
        host->RpsThrottler,
        host->MediumThrottler,
        host->TrafficMeter);
}

TClientChunkReadOptions TMultiChunkReaderHost::AdjustClientChunkReadOptions(
    const NScheduler::TClusterName& clusterName,
    const TClientChunkReadOptions& options) const
{
    if (const auto& chunkReaderStatistics = GetOrCrash(ChunkReaderStatisticsMap_, clusterName);
        chunkReaderStatistics)
    {
        auto resultOptions = options;
        resultOptions.ChunkReaderStatistics = chunkReaderStatistics;
        return resultOptions;
    }
    return options;
}

const TTrafficMeterPtr& TMultiChunkReaderHost::GetTrafficMeter() const
{
    return BaseHost_->TrafficMeter;
}

const THashMap<NScheduler::TClusterName, TChunkReaderStatisticsPtr>& TMultiChunkReaderHost::GetChunkReaderStatistics() const
{
    return ChunkReaderStatisticsMap_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
