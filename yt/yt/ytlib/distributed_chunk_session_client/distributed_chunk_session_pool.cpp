#include "distributed_chunk_session_pool.h"

#include "config.h"
#include "distributed_chunk_session_controller.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <yt/yt/core/misc/backoff_strategy.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

#include <util/random/random.h>

#include <algorithm>

namespace NYT::NDistributedChunkSessionClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;

using NApi::NNative::IClientPtr;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int ActiveSessionIdsInlineCapacity = 3;
constexpr int AllSessionIdsInlineCapacity = 6;

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionPool
    : public IDistributedChunkSessionPool
{
public:
    TDistributedChunkSessionPool(
        TDistributedChunkSessionPoolConfigPtr config,
        TCreateControllerCallback createController,
        TSendChunkSealRequestCallback sendChunkSealRequest,
        IInvokerPtr invoker,
        TLogger logger = DistributedChunkSessionLogger())
        : Config_(std::move(config))
        , CreateController_(std::move(createController))
        , SendChunkSealRequest_(std::move(sendChunkSealRequest))
        , SerializedInvoker_(CreateSerializedInvoker(std::move(invoker)))
        , Logger(std::move(logger))
    { }

    TFuture<TSessionDescriptor> GetSession(
        int slotCookie,
        std::optional<TSessionId> excludedSessionId) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return BIND_NO_PROPAGATE(
            &TDistributedChunkSessionPool::DoGetSession,
            MakeStrong(this),
            slotCookie,
            excludedSessionId)
            .AsyncVia(SerializedInvoker_)
            .Run()
            .ToUncancelable();
    }

    TFuture<void> FinalizeSlot(int slotCookie) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return BIND_NO_PROPAGATE(
            &TDistributedChunkSessionPool::DoFinalizeSlot,
            MakeStrong(this),
            slotCookie)
            .AsyncVia(SerializedInvoker_)
            .Run()
            .ToUncancelable();
    }

    TFuture<std::vector<TSlotChunkInfo>> GetSlotChunks(int slotCookie) const final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return BIND_NO_PROPAGATE(
            &TDistributedChunkSessionPool::DoGetSlotChunks,
            MakeStrong(this),
            slotCookie)
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

private:
    struct TSessionEntry
    {
        TStartedSessionInfo StartedSession;
        IDistributedChunkSessionControllerPtr Controller;
        bool Active = true;
        bool SealScheduled = false;
        std::optional<TBackoffStrategy> SealRetryBackoff;
    };

    struct TSlotState
    {
        THashMap<TSessionId, TSessionEntry> Sessions;
        TCompactVector<TSessionId, ActiveSessionIdsInlineCapacity> ActiveSessionIds;
        TCompactVector<TSessionId, AllSessionIdsInlineCapacity> AllSessionIds;
        TCompactVector<std::pair<int, TFuture<TSessionDescriptor>>, ActiveSessionIdsInlineCapacity> PendingSessions;
        bool Finalized = false;
    };

    const TDistributedChunkSessionPoolConfigPtr Config_;
    const TCreateControllerCallback CreateController_;
    const TSendChunkSealRequestCallback SendChunkSealRequest_;
    const IInvokerPtr SerializedInvoker_;
    const TLogger Logger;

    THashMap<int, TSlotState> Slots_;
    int NextPendingSessionToken_ = 0;

    std::optional<TChunkId> MaybeMarkSessionSealed(TNonNullPtr<TSessionEntry> entry) const
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (entry->SealScheduled) {
            return std::nullopt;
        }

        entry->SealScheduled = true;
        return entry->StartedSession.SessionId.ChunkId;
    }

    std::optional<TSessionId> PickActiveSession(
        const TSlotState& slot,
        std::optional<TSessionId> excludedSessionId) const
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (!excludedSessionId) {
            if (slot.ActiveSessionIds.empty()) {
                return std::nullopt;
            }

            return slot.ActiveSessionIds[RandomNumber(slot.ActiveSessionIds.size())];
        }

        int candidateSessionCount = 0;
        for (const auto& sessionId : slot.ActiveSessionIds) {
            if (sessionId != *excludedSessionId) {
                ++candidateSessionCount;
            }
        }

        if (candidateSessionCount == 0) {
            return std::nullopt;
        }

        int candidateIndex = RandomNumber<ui32>(candidateSessionCount);
        for (const auto& sessionId : slot.ActiveSessionIds) {
            if (sessionId == *excludedSessionId) {
                continue;
            }

            if (candidateIndex == 0) {
                return sessionId;
            }

            --candidateIndex;
        }

        YT_ABORT();
    }

    TFuture<TSessionDescriptor> MakeDescriptorFuture(
        const TSlotState& slot,
        TSessionId sessionId) const
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        const auto& entry = GetOrCrash(slot.Sessions, sessionId);
        return MakeFuture(TSessionDescriptor{
            .SessionId = entry.StartedSession.SessionId,
            .SequencerNode = entry.StartedSession.SequencerNode,
        });
    }

    TFuture<TSessionDescriptor> DoGetSession(
        int slotCookie,
        std::optional<TSessionId> excludedSessionId)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto& slot = Slots_[slotCookie];
        if (slot.Finalized) {
            YT_LOG_DEBUG("Rejecting session request for finalized slot (SlotCookie: %v)", slotCookie);
            return MakeFuture<TSessionDescriptor>(TError("Slot %v is finalized", slotCookie));
        }

        if (!excludedSessionId) {
            if (auto sessionId = PickActiveSession(slot, std::nullopt)) {
                YT_LOG_DEBUG("Returning active session (SlotCookie: %v, SessionId: %v)", slotCookie, *sessionId);
                return MakeDescriptorFuture(slot, *sessionId);
            }

            if (!slot.PendingSessions.empty()) {
                YT_LOG_DEBUG("Returning pending session future (SlotCookie: %v)", slotCookie);
                return slot.PendingSessions.front().second;
            }

            return CreateAndActivateSession(slotCookie);
        }

        if (std::ssize(slot.ActiveSessionIds) + std::ssize(slot.PendingSessions) >= Config_->MaxActiveSessionsPerSlot) {
            auto sessionId = PickActiveSession(slot, excludedSessionId);
            if (sessionId) {
                YT_LOG_DEBUG(
                    "Returning alternative active session (SlotCookie: %v, SessionId: %v, ExcludedSessionId: %v)",
                    slotCookie,
                    *sessionId,
                    *excludedSessionId);
                return MakeDescriptorFuture(slot, *sessionId);
            }

            if (slot.PendingSessions.empty()) {
                sessionId = PickActiveSession(slot, std::nullopt);
                YT_VERIFY(sessionId);

                YT_LOG_DEBUG(
                    "Returning fallback active session (SlotCookie: %v, SessionId: %v, ExcludedSessionId: %v)",
                    slotCookie,
                    *sessionId,
                    *excludedSessionId);
                return MakeDescriptorFuture(slot, *sessionId);
            }

            YT_LOG_DEBUG(
                "Returning pending session future after exclusion (SlotCookie: %v, ExcludedSessionId: %v)",
                slotCookie,
                *excludedSessionId);
            return slot.PendingSessions.front().second;
        }

        return CreateAndActivateSession(slotCookie);
    }

    TFuture<TSessionDescriptor> CreateAndActivateSession(int slotCookie)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto pendingToken = NextPendingSessionToken_++;
        auto controller = CreateController_();
        YT_LOG_DEBUG("Creating session (SlotCookie: %v, PendingToken: %v)", slotCookie, pendingToken);

        auto sessionFuture = controller->StartSession()
            .Apply(BIND_NO_PROPAGATE(
                &TDistributedChunkSessionPool::OnSessionStarted,
                MakeStrong(this),
                slotCookie,
                pendingToken,
                std::move(controller))
                .AsyncVia(SerializedInvoker_));

        auto& slot = Slots_[slotCookie];
        slot.PendingSessions.emplace_back(pendingToken, sessionFuture);
        return sessionFuture;
    }

    TSessionDescriptor OnSessionStarted(
        int slotCookie,
        int pendingToken,
        IDistributedChunkSessionControllerPtr controller,
        const TErrorOr<TStartedSessionInfo>& startedSessionOrError)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto& slot = Slots_[slotCookie];
        slot.PendingSessions.erase(
            std::remove_if(
                slot.PendingSessions.begin(),
                slot.PendingSessions.end(),
                [&] (const auto& pendingSession) {
                    return pendingSession.first == pendingToken;
                }),
            slot.PendingSessions.end());

        if (!startedSessionOrError.IsOK()) {
            YT_LOG_DEBUG(
                static_cast<const TError&>(startedSessionOrError),
                "Failed to start session (SlotCookie: %v, PendingToken: %v)",
                slotCookie,
                pendingToken);

            startedSessionOrError.ThrowOnError();
        }

        const auto& startedSession = startedSessionOrError.Value();
        auto sessionId = startedSession.SessionId;

        auto sessionIt = EmplaceOrCrash(
            slot.Sessions,
            sessionId,
            TSessionEntry{
                .StartedSession = startedSession,
                .Controller = std::move(controller),
                .Active = !slot.Finalized,
                .SealScheduled = false,
            });
        // NB: Record the session and install the close subscription before checking slot.Finalized
        // so a late start after finalization still routes through OnSessionClosed and schedules sealing.
        sessionIt->second.Controller->GetClosedFuture().Subscribe(
            BIND_NO_PROPAGATE(
                &TDistributedChunkSessionPool::OnSessionClosed,
                MakeWeak(this),
                slotCookie,
                sessionId)
                .Via(SerializedInvoker_));
        slot.AllSessionIds.push_back(sessionId);

        if (slot.Finalized) {
            YT_LOG_DEBUG(
                "Closing session started for finalized slot (SlotCookie: %v, SessionId: %v, PendingToken: %v)",
                slotCookie,
                sessionId,
                pendingToken);
            Y_UNUSED(sessionIt->second.Controller->Close());
            THROW_ERROR_EXCEPTION("Slot %v is finalized", slotCookie);
        }

        slot.ActiveSessionIds.push_back(sessionId);

        YT_LOG_DEBUG(
            "Session started (SlotCookie: %v, SessionId: %v, PendingToken: %v)",
            slotCookie,
            sessionId,
            pendingToken);

        return TSessionDescriptor{
            .SessionId = startedSession.SessionId,
            .SequencerNode = startedSession.SequencerNode,
        };
    }

    void OnSessionClosed(
        int slotCookie,
        TSessionId sessionId,
        const TError& closeError)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto slotIt = Slots_.find(slotCookie);
        if (slotIt == Slots_.end()) {
            return;
        }

        auto& slot = slotIt->second;
        auto sessionIt = slot.Sessions.find(sessionId);
        if (sessionIt == slot.Sessions.end()) {
            return;
        }

        auto& entry = sessionIt->second;
        if (entry.Active) {
            slot.ActiveSessionIds.erase(
                std::remove(slot.ActiveSessionIds.begin(), slot.ActiveSessionIds.end(), sessionId),
                slot.ActiveSessionIds.end());
            entry.Active = false;
        }

        YT_LOG_DEBUG(closeError, "Session closed (SlotCookie: %v, SessionId: %v)", slotCookie, sessionId);

        if (auto chunkId = MaybeMarkSessionSealed(&entry)) {
            ScheduleChunkSeal(slotCookie, sessionId, *chunkId);
        }
    }

    void DoFinalizeSlot(int slotCookie)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<std::pair<TSessionId, IDistributedChunkSessionControllerPtr>> activeSessions;
        std::vector<TSessionId> sessionsToSeal;
        auto& slot = Slots_[slotCookie];
        if (slot.Finalized) {
            YT_LOG_DEBUG("Slot is already finalized (SlotCookie: %v)", slotCookie);
            return;
        }

        slot.Finalized = true;
        auto sessionCount = std::ssize(slot.AllSessionIds);
        activeSessions.reserve(slot.AllSessionIds.size());

        for (const auto& sessionId : slot.AllSessionIds) {
            auto& entry = GetOrCrash(slot.Sessions, sessionId);
            if (entry.Active) {
                activeSessions.emplace_back(sessionId, entry.Controller);
            }

            entry.Active = false;
            if (MaybeMarkSessionSealed(&entry)) {
                sessionsToSeal.push_back(sessionId);
            }
        }

        slot.ActiveSessionIds.clear();

        YT_LOG_DEBUG(
            "Finalizing slot (SlotCookie: %v, SessionCount: %v, ActiveSessionCount: %v, SealedChunkCount: %v)",
            slotCookie,
            sessionCount,
            std::ssize(activeSessions),
            std::ssize(sessionsToSeal));

        // TODO(apollo1321): For now session close and chunk seal scheduling run in parallel.
        // Finalize through sequencer stats first and seal directly when close succeeds; keep
        // ScheduleChunkSeal as a fallback for sessions that cannot be closed cleanly.
        for (const auto& [sessionId, controller] : activeSessions) {
            controller->Close().Subscribe(
                BIND_NO_PROPAGATE(
                    &TDistributedChunkSessionPool::OnSessionCloseFailedDuringFinalize,
                    MakeStrong(this),
                    slotCookie,
                    sessionId)
                    .Via(SerializedInvoker_));
        }

        // TODO(apollo1321): Batch chunk seal scheduling instead of issuing one request per chunk.
        for (auto sessionId : sessionsToSeal) {
            ScheduleChunkSeal(slotCookie, sessionId, sessionId.ChunkId);
        }

        YT_LOG_DEBUG(
            "Slot finalized (SlotCookie: %v, SealedChunkCount: %v)",
            slotCookie,
            std::ssize(sessionsToSeal));
    }

    void OnSessionCloseFailedDuringFinalize(
        int slotCookie,
        TSessionId sessionId,
        const TError& error)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (error.IsOK()) {
            return;
        }

        YT_LOG_WARNING(
            error,
            "Session close failed during slot finalization (SlotCookie: %v, SessionId: %v)",
            slotCookie,
            sessionId);
    }

    void ScheduleChunkSeal(int slotCookie, TSessionId sessionId, TChunkId chunkId)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        YT_LOG_DEBUG(
            "Scheduling chunk sealing (SlotCookie: %v, SessionId: %v, ChunkId: %v)",
            slotCookie,
            sessionId,
            chunkId);

        SendChunkSealRequest_(chunkId)
            .Subscribe(BIND_NO_PROPAGATE(
                &TDistributedChunkSessionPool::OnChunkSealScheduled,
                MakeStrong(this),
                slotCookie,
                sessionId,
                chunkId)
                .Via(SerializedInvoker_));
    }

    void OnChunkSealScheduled(
        int slotCookie,
        TSessionId sessionId,
        TChunkId chunkId,
        const TError& error)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (error.IsOK()) {
            auto slotIt = Slots_.find(slotCookie);
            YT_VERIFY(slotIt != Slots_.end());

            auto sessionIt = slotIt->second.Sessions.find(sessionId);
            YT_VERIFY(sessionIt != slotIt->second.Sessions.end());

            sessionIt->second.SealRetryBackoff.reset();

            YT_LOG_DEBUG(
                "Chunk sealing scheduled (SlotCookie: %v, SessionId: %v, ChunkId: %v)",
                slotCookie,
                sessionId,
                chunkId);
            return;
        }

        auto slotIt = Slots_.find(slotCookie);
        YT_VERIFY(slotIt != Slots_.end());

        auto sessionIt = slotIt->second.Sessions.find(sessionId);
        YT_VERIFY(sessionIt != slotIt->second.Sessions.end());

        auto& sealRetryBackoff = sessionIt->second.SealRetryBackoff;
        if (!sealRetryBackoff) {
            sealRetryBackoff.emplace(Config_->ChunkSealRetryBackoff);
        }

        if (!sealRetryBackoff->Next()) {
            YT_LOG_ALERT(
                error,
                "Failed to schedule chunk sealing; retries exhausted (SlotCookie: %v, SessionId: %v, ChunkId: %v, InvocationCount: %v)",
                slotCookie,
                sessionId,
                chunkId,
                sealRetryBackoff->GetInvocationCount());
            return;
        }

        const TDuration retryBackoff = sealRetryBackoff->GetBackoff();

        YT_LOG_WARNING(
            error,
            "Failed to schedule chunk sealing; retrying (SlotCookie: %v, SessionId: %v, ChunkId: %v, RetryIndex: %v, RetryBackoff: %v)",
            slotCookie,
            sessionId,
            chunkId,
            sealRetryBackoff->GetInvocationIndex(),
            retryBackoff);

        TDelayedExecutor::Submit(
            BIND_NO_PROPAGATE(
                &TDistributedChunkSessionPool::ScheduleChunkSeal,
                MakeWeak(this),
                slotCookie,
                sessionId,
                chunkId)
                .Via(SerializedInvoker_),
            retryBackoff);
    }

    std::vector<TSlotChunkInfo> DoGetSlotChunks(int slotCookie) const
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto it = Slots_.find(slotCookie);
        if (it == Slots_.end()) {
            return {};
        }

        std::vector<TSlotChunkInfo> result;
        result.reserve(it->second.AllSessionIds.size());
        for (const auto& sessionId : it->second.AllSessionIds) {
            const auto& entry = GetOrCrash(it->second.Sessions, sessionId);
            result.push_back(TSlotChunkInfo{
                .ChunkId = entry.StartedSession.SessionId.ChunkId,
                .Replicas = entry.StartedSession.Replicas,
            });
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionPoolPtr CreateDistributedChunkSessionPool(
    IClientPtr client,
    TDistributedChunkSessionPoolConfigPtr config,
    TDistributedChunkSessionControllerConfigPtr controllerConfig,
    TTransactionId transactionId,
    NApi::TJournalChunkWriterOptionsPtr writerOptions,
    NApi::TJournalChunkWriterConfigPtr writerConfig,
    IInvokerPtr invoker,
    TLogger logger)
{
    const auto Logger = logger;
    const auto chunkSealRpcTimeout = config->ChunkSealRpcTimeout;

    auto createController = BIND([
        client,
        controllerConfig,
        transactionId,
        writerOptions,
        writerConfig,
        invoker
    ] {
        return CreateDistributedChunkSessionController(
            client,
            controllerConfig,
            transactionId,
            writerOptions,
            writerConfig,
            invoker);
    });

    auto sendChunkSealRequest = BIND_NO_PROPAGATE([client, chunkSealRpcTimeout, Logger] (TChunkId chunkId) {
        YT_LOG_DEBUG("Sending chunk seal request (ChunkId: %v)", chunkId);

        auto channel = client->GetMasterChannelOrThrow(
            NApi::EMasterChannelKind::Leader,
            CellTagFromId(chunkId));
        TChunkServiceProxy proxy(channel);

        auto req = proxy.ScheduleChunkSeal();
        req->SetTimeout(chunkSealRpcTimeout);
        GenerateMutationId(req);
        ToProto(req->mutable_chunk_id(), chunkId);

        auto future = req->Invoke().AsVoid();
        future.Subscribe(BIND_NO_PROPAGATE([Logger, chunkId] (const TError& error) {
            if (error.IsOK()) {
                YT_LOG_DEBUG("Chunk seal request succeeded (ChunkId: %v)", chunkId);
            } else {
                YT_LOG_WARNING(error, "Chunk seal request failed (ChunkId: %v)", chunkId);
            }
        }));

        return future;
    });

    return New<TDistributedChunkSessionPool>(
        std::move(config),
        std::move(createController),
        std::move(sendChunkSealRequest),
        std::move(invoker),
        std::move(logger));
}

IDistributedChunkSessionPoolPtr CreateDistributedChunkSessionPoolForTesting(
    TDistributedChunkSessionPoolConfigPtr config,
    TDistributedChunkSessionPoolTestingOptions options,
    IInvokerPtr invoker,
    TLogger logger)
{
    return New<TDistributedChunkSessionPool>(
        std::move(config),
        std::move(options.CreateController),
        std::move(options.SendChunkSealRequest),
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
