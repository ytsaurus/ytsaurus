#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/actions/public.h>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

struct TSessionDescriptor
{
    NChunkClient::TSessionId SessionId;
    NNodeTrackerClient::TNodeDescriptor SequencerNode;
};

struct TSlotChunkInfo
{
    NChunkClient::TChunkId ChunkId;
    NChunkClient::TChunkReplicaWithMediumList Replicas;
};

//! Thread affinity: any.
struct IDistributedChunkSessionPool
    : virtual public TRefCounted
{
    virtual TFuture<TSessionDescriptor> GetSession(
        int slotCookie,
        std::optional<NChunkClient::TSessionId> excludedSessionId = {}) = 0;

    virtual TFuture<void> FinalizeSlot(int slotCookie) = 0;

    virtual TFuture<std::vector<TSlotChunkInfo>> GetSlotChunks(int slotCookie) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedChunkSessionPool)

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionPoolPtr CreateDistributedChunkSessionPool(
    NApi::NNative::IClientPtr client,
    TDistributedChunkSessionPoolConfigPtr config,
    TDistributedChunkSessionControllerConfigPtr controllerConfig,
    NObjectClient::TTransactionId transactionId,
    NApi::TJournalChunkWriterOptionsPtr writerOptions,
    NApi::TJournalChunkWriterConfigPtr writerConfig,
    IInvokerPtr invoker,
    NLogging::TLogger logger = DistributedChunkSessionLogger());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
