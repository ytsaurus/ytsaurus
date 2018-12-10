#include "erasure_reader.h"
#include "block_cache.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader.h"
#include "chunk_writer.h"
#include "config.h"
#include "dispatcher.h"
#include "erasure_helpers.h"
#include "block.h"
#include "replication_reader.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/client/api/config.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/erasure/codec.h>
#include <yt/core/erasure/helpers.h>

#include <numeric>

namespace NYT::NChunkClient {

using namespace NApi;
using namespace NErasure;
using namespace NConcurrency;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NErasureHelpers;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::vector<IChunkReaderPtr> CreateErasurePartsReaders(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TChunkId& chunkId,
    const TChunkReplicaList& replicas,
    const ICodec* codec,
    int partCount,
    IBlockCachePtr blockCache,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler)
{
    YCHECK(IsErasureChunkId(chunkId));

    auto sortedReplicas = replicas;
    std::sort(
        sortedReplicas.begin(),
        sortedReplicas.end(),
        [] (TChunkReplica lhs, TChunkReplica rhs) {
            return lhs.GetReplicaIndex() < rhs.GetReplicaIndex();
        });

    std::vector<IChunkReaderPtr> readers;
    readers.reserve(partCount);

    {
        auto it = sortedReplicas.begin();
        while (it != sortedReplicas.end() && it->GetReplicaIndex() < partCount) {
            auto jt = it;
            while (jt != sortedReplicas.end() &&
                   it->GetReplicaIndex() == jt->GetReplicaIndex())
            {
                ++jt;
            }

            TChunkReplicaList partReplicas(it, jt);
            auto partId = ErasurePartIdFromChunkId(chunkId, it->GetReplicaIndex());
            auto reader = CreateReplicationReader(
                config,
                options,
                client,
                nodeDirectory,
                // Locality doesn't matter, since we typically have only one replica.
                TNodeDescriptor(),
                partId,
                partReplicas,
                blockCache,
                trafficMeter,
                bandwidthThrottler,
                rpsThrottler);
            readers.push_back(reader);

            it = jt;
        }
    }
    YCHECK(readers.size() == partCount);

    return readers;
}

} // namespace

std::vector<IChunkReaderPtr> CreateErasureAllPartsReaders(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas,
    const ICodec* codec,
    IBlockCachePtr blockCache,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler)
{
    return CreateErasurePartsReaders(
        config,
        options,
        client,
        nodeDirectory,
        chunkId,
        seedReplicas,
        codec,
        codec->GetTotalPartCount(),
        blockCache,
        std::move(trafficMeter),
        std::move(bandwidthThrottler),
        std::move(rpsThrottler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

