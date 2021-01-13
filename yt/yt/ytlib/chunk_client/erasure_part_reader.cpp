#include "erasure_part_reader.h"
#include "block_cache.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader.h"
#include "chunk_writer.h"
#include "config.h"
#include "erasure_helpers.h"
#include "block.h"
#include "replication_reader.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/client/api/config.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/library/erasure/codec.h>

#include <numeric>

namespace NYT::NChunkClient {

using namespace NApi;
using namespace NErasure;
using namespace NConcurrency;
using namespace NYTree;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NErasureHelpers;

////////////////////////////////////////////////////////////////////////////////

std::vector<IChunkReaderPtr> CreateErasurePartReaders(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    TChunkId chunkId,
    const TChunkReplicaList& replicas,
    const ICodec* codec,
    const TPartIndexList& partIndexList,
    IBlockCachePtr blockCache,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler)
{
    YT_VERIFY(IsErasureChunkId(chunkId));
    YT_VERIFY(std::is_sorted(partIndexList.begin(), partIndexList.end()));

    auto totalPartCount = codec->GetTotalPartCount();
    THashSet<int> partIndexSet(partIndexList.begin(), partIndexList.end());
    YT_VERIFY(partIndexSet.size() == partIndexList.size());

    auto sortedReplicas = replicas;
    std::sort(
        sortedReplicas.begin(),
        sortedReplicas.end(),
        [] (TChunkReplica lhs, TChunkReplica rhs) {
            return lhs.GetReplicaIndex() < rhs.GetReplicaIndex();
        });

    auto partConfig = CloneYsonSerializable(config);
    partConfig->FailOnNoSeeds = true;

    std::vector<IChunkReaderPtr> readers;
    readers.reserve(partIndexSet.size());

    {
        auto it = sortedReplicas.begin();
        while (it != sortedReplicas.end() && it->GetReplicaIndex() < totalPartCount) {
            auto jt = it;
            while (jt != sortedReplicas.end() &&
                   it->GetReplicaIndex() == jt->GetReplicaIndex())
            {
                ++jt;
            }

            if (partIndexSet.contains(it->GetReplicaIndex())) {
                TChunkReplicaList partReplicas(it, jt);
                auto partChunkId = ErasurePartIdFromChunkId(chunkId, it->GetReplicaIndex());
                auto reader = CreateReplicationReader(
                    partConfig,
                    options,
                    client,
                    nodeDirectory,
                    // Locality doesn't matter, since we typically have only one replica.
                    /* localDescriptor */ {},
                    /* partitionTag */ std::nullopt,
                    partChunkId,
                    partReplicas,
                    blockCache,
                    trafficMeter,
                    bandwidthThrottler,
                    rpsThrottler);
                readers.push_back(reader);
            }

            it = jt;
        }
    }
    YT_VERIFY(readers.size() == partIndexSet.size());

    return readers;
}

std::vector<IChunkReaderPtr> CreateAllErasurePartReaders(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    TChunkId chunkId,
    const TChunkReplicaList& seedReplicas,
    const ICodec* codec,
    IBlockCachePtr blockCache,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler)
{
    auto partCount = codec->GetTotalPartCount();
    TPartIndexList partIndexList(partCount);
    std::iota(partIndexList.begin(), partIndexList.end(), 0);

    return CreateErasurePartReaders(
        config,
        options,
        client,
        nodeDirectory,
        chunkId,
        seedReplicas,
        codec,
        partIndexList,
        blockCache,
        std::move(trafficMeter),
        std::move(bandwidthThrottler),
        std::move(rpsThrottler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

