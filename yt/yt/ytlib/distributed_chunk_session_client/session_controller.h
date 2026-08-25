#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

struct TStartedSessionInfo
{
    NChunkClient::TSessionId SessionId;
    NNodeTrackerClient::TNodeDescriptor SequencerNode;
    NChunkClient::TChunkReplicaWithMediumList Replicas;
};

////////////////////////////////////////////////////////////////////////////////

struct IDistributedChunkSessionController
    : virtual public TRefCounted
{
    // Starts session and returns write-session metadata.
    virtual TFuture<TStartedSessionInfo> StartSession() = 0;

    virtual TFuture<void> Close() = 0;

    virtual TFuture<void> GetClosedFuture() = 0;

    virtual NChunkClient::TSessionId GetSessionId() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedChunkSessionController)

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionControllerPtr CreateDistributedChunkSessionController(
    NApi::NNative::IClientPtr client,
    TDistributedChunkSessionControllerConfigPtr config,
    NObjectClient::TTransactionId transactionId,
    NApi::TJournalChunkWriterOptionsPtr writerOptions,
    NApi::TJournalChunkWriterConfigPtr writerConfig,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
