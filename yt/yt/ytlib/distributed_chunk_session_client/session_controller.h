#pragma once

#include "public.h"
#include "statistics.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/actions/signal.h>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

struct TStartedSessionInfo
{
    NChunkClient::TSessionId SessionId;
    NNodeTrackerClient::TNodeDescriptor SequencerNode;
    NChunkClient::TChunkReplicaWithMediumList Replicas;
};

using TSessionProgressUpdatedSignature =
    void(const TControllerSessionProgress& progress);

////////////////////////////////////////////////////////////////////////////////

//! Owns the session protocol with one sequencer node: start, lease pings, close.
//! Chunk lifecycle -- creation, sealing, metadata retention -- belongs to the pool.
// TODO(apollo1321): Move chunk creation and target allocation to the pool too.
struct IDistributedChunkSessionController
    : virtual public TRefCounted
{
    // Starts session and returns write-session metadata.
    virtual TFuture<TStartedSessionInfo> StartSession() = 0;

    //! Must not be called before StartSession() has succeeded.
    virtual TFuture<void> Close() = 0;

    virtual TFuture<void> GetClosedFuture() = 0;

    //! Valid once StartSession() has succeeded, including while the session is closing;
    //! null until then.
    virtual NChunkClient::TSessionId GetSessionId() const = 0;

    //! Reports session progress. Terminal delivery does not depend on who called Close().
    /*!
     *  InFlight* (Final | CloseFailed)
     *
     *  Every subscriber receives the terminal alternative exactly once, whenever it
     *  subscribed. It is raised before the closed future resolves.
     */
    DECLARE_INTERFACE_SIGNAL(TSessionProgressUpdatedSignature, ProgressUpdated);
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
