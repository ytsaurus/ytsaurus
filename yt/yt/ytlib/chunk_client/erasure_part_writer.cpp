#include "erasure_writer.h"

#include "block_reorderer.h"
#include "chunk_meta_extensions.h"
#include "chunk_writer.h"
#include "config.h"
#include "deferred_chunk_meta.h"
#include "dispatcher.h"
#include "replication_writer.h"
#include "helpers.h"
#include "erasure_helpers.h"
#include "block.h"
#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NChunkClient {

using namespace NErasure;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NErasureHelpers;

////////////////////////////////////////////////////////////////////////////////

std::vector<IChunkWriterPtr> CreateErasurePartWriters(
    TReplicationWriterConfigPtr config,
    TRemoteWriterOptionsPtr options,
    TSessionId sessionId,
    NApi::NNative::IClientPtr client,
    const TPartIndexList& partIndexList,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache,
    TChunkReplicaWithMediumList targetReplicas)
{
    auto partConfig = NYTree::CloneYsonStruct(config);
    // Ignore upload replication factor for erasure chunk parts.
    partConfig->UploadReplicationFactor = 1;

    // NB: Don't prefer local host: if the latter is unhealthy and cannot start writing a chunk
    // we have no way of retrying the allocation.

    if (targetReplicas.empty()) {
        targetReplicas = AllocateWriteTargets(
            client,
            sessionId,
            partIndexList.size(),
            partIndexList.size(),
            /*replicationFactorOverride*/ std::nullopt,
            /*preferredHostName*/ std::nullopt,
            /*forbiddenAddresses*/ {},
            /*allocatedAddresses*/ {},
            ChunkClientLogger);
    }

    YT_VERIFY(targetReplicas.size() == partIndexList.size());

    std::vector<IChunkWriterPtr> writers;
    for (int index = 0; index < std::ssize(partIndexList); ++index) {
        auto partIndex = partIndexList[index];
        auto partSessionId = TSessionId(
            ErasurePartIdFromChunkId(sessionId.ChunkId, partIndex),
            sessionId.MediumIndex);
        // NB: the order of replicas is significant (at least for consistently
        // placed chunks).
        writers.push_back(CreateReplicationWriter(
            partConfig,
            options,
            partSessionId,
            TChunkReplicaWithMediumList(1, targetReplicas[index]),
            client,
            /*localHostName*/ TString(), // Locality is not important here.
            blockCache,
            trafficMeter,
            throttler));
    }

    return writers;
}

std::vector<IChunkWriterPtr> CreateAllErasurePartWriters(
    TReplicationWriterConfigPtr config,
    TRemoteWriterOptionsPtr options,
    TSessionId sessionId,
    ICodec* codec,
    NApi::NNative::IClientPtr client,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache,
    TChunkReplicaWithMediumList targetReplicas)
{
    auto totalPartCount = codec->GetTotalPartCount();
    TPartIndexList partIndexList(totalPartCount);
    std::iota(partIndexList.begin(), partIndexList.end(), 0);

    return CreateErasurePartWriters(
        config,
        options,
        sessionId,
        client,
        partIndexList,
        trafficMeter,
        throttler,
        blockCache,
        std::move(targetReplicas));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
