#include "chunk_reader_host.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

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
    TTrafficMeterPtr trafficMeter,
    std::optional<THashMap<TString, NConcurrency::IThroughputThrottlerPtr>> perClusterInBandwidthThrottlers)
    : Client(std::move(client))
    , LocalDescriptor(std::move(localDescriptor))
    , BlockCache(std::move(blockCache))
    , ChunkMetaCache(std::move(chunkMetaCache))
    , NodeStatusDirectory(std::move(nodeStatusDirectory))
    , BandwidthThrottler(std::move(bandwidthThrottler))
    , RpsThrottler(std::move(rpsThrottler))
    , MediumThrottler(std::move(mediumThrottler))
    , TrafficMeter(std::move(trafficMeter))
    , PerClusterInBandwidthThrottlers(std::move(perClusterInBandwidthThrottlers))
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
        /*trafficMeter*/ nullptr,
        /*perClusterInBandwidthThrottlers*/ std::nullopt);
}

TChunkReaderHostPtr TChunkReaderHost::CreateHostForCluster(const TClusterName& clusterName) const
{
    auto client = Client;
    auto bandwidthThrottler = BandwidthThrottler;

    std::optional<TString> remoteClusterName;
    if (!IsLocal(clusterName)) {
        client = Client
                ->GetNativeConnection()
                ->GetClusterDirectory()
                ->GetConnectionOrThrow(clusterName.Underlying())
                ->CreateNativeClient(Client->GetOptions());

        remoteClusterName = clusterName.Underlying();
    }

    if (remoteClusterName && PerClusterInBandwidthThrottlers) {
        if (auto it = PerClusterInBandwidthThrottlers->find(*remoteClusterName); it != PerClusterInBandwidthThrottlers->end()) {
            bandwidthThrottler = it->second;
        }
    }

    return New<TChunkReaderHost>(
        std::move(client),
        LocalDescriptor,
        BlockCache,
        ChunkMetaCache,
        NodeStatusDirectory,
        std::move(bandwidthThrottler),
        RpsThrottler,
        MediumThrottler,
        TrafficMeter,
        PerClusterInBandwidthThrottlers);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
