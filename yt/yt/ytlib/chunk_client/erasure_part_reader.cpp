#include "erasure_part_reader.h"
#include "block_cache.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader.h"
#include "chunk_writer.h"
#include "config.h"
#include "erasure_helpers.h"
#include "block.h"
#include "replication_reader.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/library/erasure/impl/codec.h>

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

std::vector<IChunkReaderAllowingRepairPtr> CreateErasurePartReaders(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NNative::IClientPtr client,
    TChunkId chunkId,
    const TChunkReplicaList& replicas,
    const TPartIndexList& partIndexList,
    IBlockCachePtr blockCache,
    IClientChunkMetaCachePtr chunkMetaCache,
    EUnavailablePartPolicy unavailablePartPolicy,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler)
{
    YT_VERIFY(IsErasureChunkId(chunkId));
    YT_VERIFY(std::is_sorted(partIndexList.begin(), partIndexList.end()));

    auto sortedReplicas = replicas;
    std::sort(
        sortedReplicas.begin(),
        sortedReplicas.end(),
        [] (TChunkReplica lhs, TChunkReplica rhs) {
            return lhs.GetReplicaIndex() < rhs.GetReplicaIndex();
        });

    auto partConfig = CloneYsonSerializable(config);
    partConfig->FailOnNoSeeds = true;

    std::vector<IChunkReaderAllowingRepairPtr> readers;
    readers.reserve(partIndexList.size());

    auto it = sortedReplicas.begin();

    for (auto partIndex : partIndexList) {
        while (it != sortedReplicas.end() && it->GetReplicaIndex() < partIndex) {
            ++it;
        }

        if (it != sortedReplicas.end() && it->GetReplicaIndex() == partIndex) {
            auto jt = it;
            while (jt != sortedReplicas.end() && jt->GetReplicaIndex() == partIndex) {
                ++jt;
            }

            TChunkReplicaList partReplicas(it, jt);
            auto partChunkId = ErasurePartIdFromChunkId(chunkId, it->GetReplicaIndex());
            auto reader = CreateReplicationReader(
                partConfig,
                options,
                client,
                // Locality doesn't matter, since we typically have only one replica.
                /*localDescriptor*/ {},
                partChunkId,
                partReplicas,
                blockCache,
                chunkMetaCache,
                trafficMeter,
                /*nodeStatusDirectory*/ nullptr,
                bandwidthThrottler,
                rpsThrottler);
            readers.push_back(reader);

            it = jt;
        } else {
            switch (unavailablePartPolicy) {
                case EUnavailablePartPolicy::CreateNullReader:
                    readers.push_back(nullptr);
                    break;

                case EUnavailablePartPolicy::Crash:
                default:
                    YT_ABORT();
            }
        }
    }

    return readers;
}

std::vector<IChunkReaderAllowingRepairPtr> CreateAllErasurePartReaders(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NNative::IClientPtr client,
    TChunkId chunkId,
    const TChunkReplicaList& seedReplicas,
    const ICodec* codec,
    IBlockCachePtr blockCache,
    IClientChunkMetaCachePtr chunkMetaCache,
    EUnavailablePartPolicy unavailablePartPolicy,
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
        chunkId,
        seedReplicas,
        partIndexList,
        blockCache,
        chunkMetaCache,
        unavailablePartPolicy,
        std::move(trafficMeter),
        std::move(bandwidthThrottler),
        std::move(rpsThrottler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

