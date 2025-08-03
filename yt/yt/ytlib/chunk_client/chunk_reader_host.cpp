#include "chunk_reader_host.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

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

THashMap<TClusterName, TChunkReaderHostPtr> CreateHostsMap(
    const TChunkReaderHostPtr& baseHost,
    const std::vector<TClusterName>& clusterList)
{
    THashMap<TClusterName, TChunkReaderHostPtr> hosts;
    const auto& baseClient = baseHost->Client;
    for (const auto& clusterName : clusterList) {
        auto client = baseClient;

        if (!IsLocal(clusterName)) {
            client = baseClient
                    ->GetNativeConnection()
                    ->GetClusterDirectory()
                    ->GetConnectionOrThrow(clusterName.Underlying())
                    ->CreateNativeClient(baseClient->GetOptions());
        }

        hosts[clusterName] = New<TChunkReaderHost>(
            std::move(client),
            baseHost->LocalDescriptor,
            baseHost->BlockCache,
            baseHost->ChunkMetaCache,
            baseHost->NodeStatusDirectory,
            baseHost->BandwidthThrottler,
            baseHost->RpsThrottler,
            baseHost->MediumThrottler,
            baseHost->TrafficMeter);
    }
    return hosts;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TMultiChunkReaderHost::TMultiChunkReaderHost(
    TChunkReaderHostPtr baseHost,
    TBandwidthThrottlerFactory bandwidthThrottlerFactory,
    const std::vector<TClusterName>& clusterList)
    : BaseHost_(std::move(baseHost))
    , BandwidthThrottlerFactory_(std::move(bandwidthThrottlerFactory))
    , Hosts_(CreateHostsMap(BaseHost_, clusterList))
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

TTrafficMeterPtr TMultiChunkReaderHost::GetTrafficMeter() const
{
    return BaseHost_->TrafficMeter;
}

TMultiChunkReaderHostPtr CreateMultiChunkReaderHost(
    TChunkReaderHostPtr baseHost,
    TMultiChunkReaderHost::TBandwidthThrottlerFactory bandwidthThrottlerFactory,
    const std::vector<NScheduler::TClusterName>& clusterList)
{
    YT_VERIFY(AnyOf(clusterList, [] (const auto& clusterName) { return IsLocal(clusterName); }));
    return New<TMultiChunkReaderHost>(
        std::move(baseHost),
        std::move(bandwidthThrottlerFactory),
        clusterList);
}

TMultiChunkReaderHostPtr CreateSingleSourceMultiChunkReaderHost(
    TChunkReaderHostPtr baseHost)
{
    return CreateMultiChunkReaderHost(
        std::move(baseHost),
        /*bandwidthThrottlerFactory*/ {},
        /*clusterList*/ {LocalClusterName});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
