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

#include <yt/client/api/client.h>

#include <yt/ytlib/chunk_client/proto/chunk_info.pb.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/library/erasure/codec.h>

#include <yt/core/misc/numeric_helpers.h>

#include <yt/core/rpc/dispatcher.h>

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
    ICodec* codec,
    TNodeDirectoryPtr nodeDirectory,
    NApi::NNative::IClientPtr client,
    const TPartIndexList& partIndexList,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    auto partConfig = NYTree::CloneYsonSerializable(config);
    // Ignore upload replication factor for erasure chunk parts.
    partConfig->UploadReplicationFactor = 1;
    // Don't prefer local host: if the latter is unhealthy and cannot start writing a chunk
    // we have no way of retrying the allocation.
    partConfig->PreferLocalHost = false;

    auto replicas = AllocateWriteTargets(
        client,
        sessionId,
        partIndexList.size(),
        partIndexList.size(),
        std::nullopt,
        partConfig->PreferLocalHost,
        std::vector<TString>(),
        nodeDirectory,
        ChunkClientLogger);

    YT_VERIFY(replicas.size() == partIndexList.size());

    std::vector<IChunkWriterPtr> writers;
    for (int index = 0; index < partIndexList.size(); ++index) {
        auto partIndex = partIndexList[index];
        auto partSessionId = TSessionId(
            ErasurePartIdFromChunkId(sessionId.ChunkId, partIndex),
            sessionId.MediumIndex);
        writers.push_back(CreateReplicationWriter(
            partConfig,
            options,
            partSessionId,
            TChunkReplicaWithMediumList(1, replicas[index]),
            nodeDirectory,
            client,
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
    TNodeDirectoryPtr nodeDirectory,
    NApi::NNative::IClientPtr client,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    auto totalPartCount = codec->GetTotalPartCount();
    TPartIndexList partIndexList(totalPartCount);
    std::iota(partIndexList.begin(), partIndexList.end(), 0);

    return CreateErasurePartWriters(
        config,
        options,
        sessionId,
        codec,
        nodeDirectory,
        client,
        partIndexList,
        trafficMeter,
        throttler,
        blockCache);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
