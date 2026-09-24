#pragma once

#include "public.h"
#include "statistics.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/actions/signal.h>

#include <optional>
#include <variant>

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
    //! Latest quorum-confirmed progress reported by the sequencer.
    std::optional<TDistributedChunkSessionProgress> Progress;
};

struct TReadySession
{
    int SlotCookie = 0;
    TSessionDescriptor Descriptor;
};

using TSessionProgress = std::variant<
    TSessionInFlightProgress,
    TSessionFinalProgress,
    TSessionSealSummary,
    TSessionCloseFailed>;

struct TSessionProgressUpdate
{
    int SlotCookie = 0;
    NChunkClient::TSessionId SessionId;
    TSessionProgress Progress;
};

//! Owns chunk lifecycle across a slot's sessions: sealing, terminal recovery, and
//! retained metadata. The per-session protocol belongs to the controller.
//! Thread affinity: any.
struct IDistributedChunkSessionPool
    : virtual public TRefCounted
{
    virtual TFuture<TSessionDescriptor> GetSession(
        int slotCookie,
        std::optional<NChunkClient::TSessionId> excludedSessionId = {}) = 0;

    virtual void FinalizeSlot(int slotCookie) = 0;

    virtual TFuture<std::vector<TSlotChunkInfo>> GetSlotChunks(int slotCookie) const = 0;

    virtual TFuture<std::vector<TReadySession>> GetReadySessions() const = 0;

    //! Reports per-session progress, tagged with the slot cookie and session id.
    //! Must be subscribed to before the first session is started, since updates raised
    //! for an already finished session are not replayed. The pool must have been created
    //! with a seal monitor: subscribing without one crashes.
    /*!
     *          +----------+
     *          | InFlight |
     *          +----------+
     *        .------+------.
     *        v      v      v
     *   +-------+ +------+ +-------------+
     *   | Final | |Sealed| | CloseFailed |
     *   +-------+ +------+ +-------------+
     *
     *  At most one terminal is raised: Final on a clean close carrying progress, Sealed
     *  once master seals the chunk, CloseFailed when seal retries are exhausted. Sealed
     *  waits on the seal monitor, which retries failed master polls indefinitely, so a
     *  chunk that never seals yields no terminal at all. Sealed carries only RecordCount
     *  and the physical size, without logical counters.
     */
    DECLARE_INTERFACE_SIGNAL(
        void(const TSessionProgressUpdate& update),
        ProgressUpdated);
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
    IDistributedChunkSessionSealMonitorPtr sealMonitor,
    NLogging::TLogger logger = DistributedChunkSessionLogger());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
