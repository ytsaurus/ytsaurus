#include "chunk_reader_host.h"
#include "chunk_reader_options.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NApi;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TChunkReaderHost::TChunkReaderHost(
    NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDescriptor localDescriptor,
    IBlockCachePtr blockCache,
    IClientChunkMetaCachePtr chunkMetaCache,
    NNodeTrackerClient::INodeStatusDirectoryPtr nodeStatusDirectory,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    NConcurrency::IThroughputThrottlerPtr rpsThrottler,
    NConcurrency::IThroughputThrottlerPtr mediumThrottler,
    TTrafficMeterPtr trafficMeter)
    : Client(std::move(client))
    , LocalDescriptor(std::move(localDescriptor))
    , BlockCache(std::move(blockCache))
    , ChunkMetaCache(std::move(chunkMetaCache))
    , NodeStatusDirectory(std::move(nodeStatusDirectory))
    , BandwidthThrottler(std::move(bandwidthThrottler))
    , RpsThrottler(std::move(rpsThrottler))
    , MediumThrottler(std::move(mediumThrottler))
    , TrafficMeter(std::move(trafficMeter))
{ }

TChunkReaderHostPtr TChunkReaderHost::FromClient(
    NNative::IClientPtr client,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler,
    NConcurrency::IThroughputThrottlerPtr mediumThrottler)
{
    const auto& connection = client->GetNativeConnection();
    return New<TChunkReaderHost>(
        client,
        /*localDescriptor*/ NNodeTrackerClient::TNodeDescriptor{},
        connection->GetBlockCache(),
        connection->GetChunkMetaCache(),
        /*nodeStatusDirectory*/ nullptr,
        bandwidthThrottler,
        rpsThrottler,
        mediumThrottler,
        /*trafficMeter*/ nullptr);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

THashMap<TClusterName, TChunkReaderHostPtr> CreateHostMap(
    const TChunkReaderHostPtr& baseHost,
    const std::vector<TMultiChunkReaderHost::TClusterContext>& clusterContextList)
{
    THashMap<TClusterName, TChunkReaderHostPtr> hosts;
    for (auto& clusterOptions : clusterContextList) {
        hosts.emplace(
            clusterOptions.Name,
            New<TChunkReaderHost>(
                clusterOptions.Client,
                baseHost->LocalDescriptor,
                baseHost->BlockCache,
                baseHost->ChunkMetaCache,
                baseHost->NodeStatusDirectory,
                baseHost->BandwidthThrottler,
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

} // namespace

////////////////////////////////////////////////////////////////////////////////

TMultiChunkReaderHost::TMultiChunkReaderHost(
    TChunkReaderHostPtr baseHost,
    TBandwidthThrottlerFactory bandwidthThrottlerFactory,
    std::vector<TClusterContext> clusterContextList)
    : BaseHost_(std::move(baseHost))
    , BandwidthThrottlerFactory_(std::move(bandwidthThrottlerFactory))
    , Hosts_(CreateHostMap(BaseHost_, clusterContextList))
    , ChunkReaderStatisticsMap_(CreateChunkReaderStatistics(clusterContextList))
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
        BandwidthThrottlerFactory_
            ? BandwidthThrottlerFactory_(clusterName)
            : host->BandwidthThrottler,
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

TTrafficMeterPtr TMultiChunkReaderHost::GetTrafficMeter() const
{
    return BaseHost_->TrafficMeter;
}

const THashMap<NScheduler::TClusterName, TChunkReaderStatisticsPtr>& TMultiChunkReaderHost::GetChunkReaderStatistics() const
{
    return ChunkReaderStatisticsMap_;
}

TMultiChunkReaderHostPtr CreateMultiChunkReaderHost(
    TChunkReaderHostPtr baseHost,
    TMultiChunkReaderHost::TBandwidthThrottlerFactory bandwidthThrottlerFactory,
    std::vector<TMultiChunkReaderHost::TClusterContext> clusterContextList)
{
    YT_VERIFY(AnyOf(clusterContextList, [] (const auto& clusterOptions) { return IsLocal(clusterOptions.Name); }));
    return New<TMultiChunkReaderHost>(
        std::move(baseHost),
        std::move(bandwidthThrottlerFactory),
        std::move(clusterContextList));
}

TMultiChunkReaderHostPtr CreateSingleSourceMultiChunkReaderHost(
    TChunkReaderHostPtr baseHost)
{
    std::vector<TMultiChunkReaderHost::TClusterContext> clusterContextList = {
        TMultiChunkReaderHost::TClusterContext{
            .Name = LocalClusterName,
            .Client = baseHost->Client,
        },
    };

    return CreateMultiChunkReaderHost(
        std::move(baseHost),
        /*bandwidthThrottlerFactory*/ {},
        std::move(clusterContextList));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
