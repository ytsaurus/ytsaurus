#include "chunk_reader_host.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TChunkReaderHost::TChunkReaderHost(
    NApi::NNative::IClientPtr client,
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
    NApi::NNative::IClientPtr client,
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

} // namespace NYT::NChunkClient
