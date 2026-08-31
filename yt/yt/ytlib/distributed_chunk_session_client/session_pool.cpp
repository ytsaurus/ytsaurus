#include "session_pool.h"

#include "config.h"
#include "private.h"
#include "seal_monitor.h"
#include "session_controller.h"

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

#include <library/cpp/yt/misc/variant.h>

#include <util/random/random.h>

#include <algorithm>
#include <utility>

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
        IDistributedChunkSessionSealMonitorPtr sealMonitor,
        TLogger logger = DistributedChunkSessionLogger())
        : Config_(std::move(config))
        , CreateController_(std::move(createController))
        , SendChunkSealRequest_(std::move(sendChunkSealRequest))
        , SerializedInvoker_(CreateSerializedInvoker(std::move(invoker)))
        , Logger(std::move(logger))
    {
        if (sealMonitor) {
            SealSubscription_ = sealMonitor->Subscribe(BIND_NO_PROPAGATE(
                &TDistributedChunkSessionPool::OnChunksSealed,
                MakeWeak(this))
                    .Via(SerializedInvoker_));
        }
    }

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

    void FinalizeSlot(int slotCookie) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        SerializedInvoker_->Invoke(BIND_NO_PROPAGATE(
            &TDistributedChunkSessionPool::DoFinalizeSlot,
            MakeStrong(this),
            slotCookie));
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

    TFuture<std::vector<TReadySession>> GetReadySessions() const final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return BIND_NO_PROPAGATE(
            &TDistributedChunkSessionPool::DoGetReadySessions,
            MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

    //! NB: Terminal progress after an unclean close can only be recovered from master,
    //! so a consumer of this signal is useless without a seal monitor.
    void SubscribeProgressUpdated(
        const TCallback<void(const TSessionProgressUpdate&)>& callback) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_VERIFY(SealSubscription_);

        ProgressUpdated_.Subscribe(callback);
    }

    void UnsubscribeProgressUpdated(
        const TCallback<void(const TSessionProgressUpdate&)>& callback) final
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        ProgressUpdated_.Unsubscribe(callback);
    }

private:
    struct TSessionEntry
    {
        TStartedSessionInfo StartedSession;
        IDistributedChunkSessionControllerPtr Controller;
        std::optional<TDistributedChunkSessionProgress> Progress;
        bool SealScheduled = false;
        bool TerminalProgressReported = false;
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

    TCallbackList<void(const TSessionProgressUpdate&)> ProgressUpdated_;

    TDistributedChunkSessionSealSubscriptionPtr SealSubscription_;

    THashMap<int, TSlotState> Slots_;
    THashMap<TChunkId, std::pair<int, TSessionId>> PendingRecoveryByChunkId_;
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
            YT_TLOG_DEBUG("Rejecting session request for finalized slot")
                .With("SlotCookie", slotCookie);
            return MakeFuture<TSessionDescriptor>(TError("Slot %v is finalized", slotCookie));
        }

        if (!excludedSessionId) {
            if (auto sessionId = PickActiveSession(slot, std::nullopt)) {
                YT_TLOG_DEBUG("Returning active session")
                    .With("SlotCookie", slotCookie)
                    .With("SessionId", *sessionId);
                return MakeDescriptorFuture(slot, *sessionId);
            }

            if (!slot.PendingSessions.empty()) {
                YT_TLOG_DEBUG("Returning pending session future")
                    .With("SlotCookie", slotCookie);
                return slot.PendingSessions.front().second;
            }

            return CreateAndActivateSession(slotCookie);
        }

        if (std::ssize(slot.ActiveSessionIds) + std::ssize(slot.PendingSessions) >= Config_->MaxActiveSessionsPerSlot) {
            auto sessionId = PickActiveSession(slot, excludedSessionId);
            if (sessionId) {
                YT_TLOG_DEBUG("Returning alternative active session")
                    .With("SlotCookie", slotCookie)
                    .With("SessionId", *sessionId)
                    .With("ExcludedSessionId", *excludedSessionId);
                return MakeDescriptorFuture(slot, *sessionId);
            }

            if (slot.PendingSessions.empty()) {
                sessionId = PickActiveSession(slot, std::nullopt);
                YT_VERIFY(sessionId);

                YT_TLOG_DEBUG("Returning fallback active session")
                    .With("SlotCookie", slotCookie)
                    .With("SessionId", *sessionId)
                    .With("ExcludedSessionId", *excludedSessionId);
                return MakeDescriptorFuture(slot, *sessionId);
            }

            YT_TLOG_DEBUG("Returning pending session future after exclusion")
                .With("SlotCookie", slotCookie)
                .With("ExcludedSessionId", *excludedSessionId);
            return slot.PendingSessions.front().second;
        }

        return CreateAndActivateSession(slotCookie);
    }

    TFuture<TSessionDescriptor> CreateAndActivateSession(int slotCookie)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        int pendingToken = NextPendingSessionToken_++;
        auto controller = CreateController_();
        YT_TLOG_DEBUG("Creating session")
            .With("SlotCookie", slotCookie)
            .With("PendingToken", pendingToken);

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
            YT_TLOG_DEBUG("Failed to start session")
                .With("SlotCookie", slotCookie)
                .With("PendingToken", pendingToken)
                .With(static_cast<const TError&>(startedSessionOrError));

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
                .SealScheduled = false,
            });

        // NB: Record the session and subscribe before checking slot.Finalized, so a late
        // start after finalization still receives its terminal alternative and gets sealed.
        sessionIt->second.Controller->SubscribeProgressUpdated(
            BIND_NO_PROPAGATE(
                &TDistributedChunkSessionPool::OnSessionProgressUpdated,
                MakeWeak(this),
                slotCookie,
                sessionId)
                .Via(SerializedInvoker_));
        slot.AllSessionIds.push_back(sessionId);

        if (slot.Finalized) {
            YT_TLOG_DEBUG("Closing session started for finalized slot")
                .With("SlotCookie", slotCookie)
                .With("SessionId", sessionId)
                .With("PendingToken", pendingToken);
            YT_UNUSED_FUTURE(sessionIt->second.Controller->Close());
            THROW_ERROR_EXCEPTION("Slot %v is finalized", slotCookie);
        }

        slot.ActiveSessionIds.push_back(sessionId);

        YT_TLOG_DEBUG("Session started")
            .With("SlotCookie", slotCookie)
            .With("SessionId", sessionId)
            .With("PendingToken", pendingToken);

        return TSessionDescriptor{
            .SessionId = startedSession.SessionId,
            .SequencerNode = startedSession.SequencerNode,
        };
    }

    //! The controller only raises strictly advancing progress, so anything else is a bug.
    void ReportInFlightSessionProgress(
        int slotCookie,
        TSessionId sessionId,
        TNonNullPtr<TSessionEntry> entry,
        const TDistributedChunkSessionProgress& progress)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        YT_VERIFY(IsNonnegative(progress));
        YT_VERIFY(!entry->Progress ||
            (!IsComponentwiseLessOrEqual(progress, *entry->Progress) &&
                IsComponentwiseLessOrEqual(*entry->Progress, progress)));

        entry->Progress = progress;
        ProgressUpdated_.Fire(TSessionProgressUpdate{
            .SlotCookie = slotCookie,
            .SessionId = sessionId,
            .Progress = TSessionInFlightProgress(progress),
        });
    }

    void ReportFinalSessionProgress(
        int slotCookie,
        TSessionId sessionId,
        TNonNullPtr<TSessionEntry> entry,
        const TDistributedChunkSessionProgress& progress)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        // NB: The controller fails the session on a final value that is behind or
        // inconsistent with what it already confirmed, so this cannot be reached.
        YT_VERIFY(IsNonnegative(progress));
        YT_VERIFY(!entry->Progress || IsComponentwiseLessOrEqual(*entry->Progress, progress));

        YT_VERIFY(!std::exchange(entry->TerminalProgressReported, true));

        entry->Progress = progress;
        ProgressUpdated_.Fire(TSessionProgressUpdate{
            .SlotCookie = slotCookie,
            .SessionId = sessionId,
            .Progress = TSessionFinalProgress(*entry->Progress),
        });
    }

    void ReportSealedSessionProgress(
        int slotCookie,
        TSessionId sessionId,
        TNonNullPtr<TSessionEntry> entry,
        const TSessionSealSummary& summary)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        YT_VERIFY(IsNonnegative(summary));
        YT_VERIFY(!entry->Progress || entry->Progress->RecordCount <= summary.RecordCount);
        YT_VERIFY(!std::exchange(entry->TerminalProgressReported, true));

        ProgressUpdated_.Fire(TSessionProgressUpdate{
            .SlotCookie = slotCookie,
            .SessionId = sessionId,
            .Progress = summary,
        });
    }

    //! Raises the terminal failure alternative when no terminal progress can be recovered.
    void ReportTerminalSessionFailure(
        int slotCookie,
        TSessionId sessionId,
        TNonNullPtr<TSessionEntry> entry,
        const TError& error)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (std::exchange(entry->TerminalProgressReported, true)) {
            return;
        }

        ProgressUpdated_.Fire(TSessionProgressUpdate{
            .SlotCookie = slotCookie,
            .SessionId = sessionId,
            .Progress = TSessionCloseFailed(error),
        });
    }

    void StartTerminalRecovery(
        int slotCookie,
        TSessionId sessionId,
        TNonNullPtr<TSessionEntry> entry)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto chunkId = sessionId.ChunkId;
        if (PendingRecoveryByChunkId_.contains(chunkId) || entry->TerminalProgressReported) {
            return;
        }

        // NB: SubscribeProgressUpdated verifies the monitor, so a subscriber implies one.
        if (!SealSubscription_) {
            return;
        }

        EmplaceOrCrash(
            PendingRecoveryByChunkId_,
            chunkId,
            std::pair(slotCookie, sessionId));
        SealSubscription_->TrackChunks({chunkId});

        YT_TLOG_DEBUG("Tracking sealed chunk to recover terminal session progress")
            .With("SlotCookie", slotCookie)
            .With("SessionId", sessionId)
            .With("ChunkId", chunkId);
    }

    void OnChunksSealed(std::vector<TSessionSealSummaryWithChunkId> summaries) noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        for (const auto& summary : summaries) {
            auto pendingIt = GetIteratorOrCrash(PendingRecoveryByChunkId_, summary.ChunkId);
            auto [slotCookie, sessionId] = pendingIt->second;
            PendingRecoveryByChunkId_.erase(pendingIt);
            auto& entry = GetOrCrash(GetOrCrash(Slots_, slotCookie).Sessions, sessionId);

            YT_TLOG_FATAL_IF(
                !IsNonnegative(summary.Summary),
                "Master returned invalid distributed session seal summary")
                .With("SlotCookie", slotCookie)
                .With("SessionId", sessionId)
                .With("ChunkId", summary.ChunkId)
                .With("SealSummary", summary.Summary);

            // NB: Either acknowledged records were lost or the sequencer over-reported its
            // progress. Both mean the chunk does not hold what was confirmed at quorum.
            YT_TLOG_FATAL_IF(
                entry.Progress && summary.Summary.RecordCount < entry.Progress->RecordCount,
                "Sealed chunk record count is behind confirmed session progress")
                .With("SlotCookie", slotCookie)
                .With("SessionId", sessionId)
                .With("ChunkId", summary.ChunkId)
                .With("SealedRecordCount", summary.Summary.RecordCount)
                .With("ConfirmedRecordCount", entry.Progress->RecordCount);

            // NB: Sealing is scheduled in parallel with the session close, so a clean close
            // may have published the exact terminal result while the seal was in flight.
            // The lossy seal summary is then redundant.
            if (entry.TerminalProgressReported) {
                continue;
            }

            ReportSealedSessionProgress(slotCookie, sessionId, &entry, summary.Summary);

            YT_TLOG_DEBUG("Terminal session result recovered from sealed chunk")
                .With("SlotCookie", slotCookie)
                .With("SessionId", sessionId)
                .With("ChunkId", summary.ChunkId)
                .With("ConfirmedRecordCount", entry.Progress
                    ? std::optional(entry.Progress->RecordCount)
                    : std::nullopt)
                .With("SealSummary", summary.Summary);
        }
    }

    void OnSessionProgressUpdated(
        int slotCookie,
        TSessionId sessionId,
        const TControllerSessionProgress& controllerProgress)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto& entry = GetOrCrash(GetOrCrash(Slots_, slotCookie).Sessions, sessionId);
        if (entry.TerminalProgressReported) {
            return;
        }

        Visit(controllerProgress,
            [&] (const TSessionInFlightProgress& inFlight) {
                ReportInFlightSessionProgress(
                    slotCookie,
                    sessionId,
                    &entry,
                    inFlight.Underlying());
            },
            [&] (const TSessionFinalProgress& final) {
                const auto& progress = final.Underlying();
                // COMPAT(apollo1321): A pre-26.2 sequencer reports no final progress, so
                // the terminal result has to come from master sealing instead.
                if (progress) {
                    ReportFinalSessionProgress(slotCookie, sessionId, &entry, *progress);
                }
                OnSessionTerminated(slotCookie, sessionId, &entry);
            },
            [&] (const TSessionCloseFailed& closeFailed) {
                YT_TLOG_DEBUG("Session close failed, recovering terminal progress from master")
                    .With("SlotCookie", slotCookie)
                    .With("SessionId", sessionId)
                    .With(closeFailed.Underlying());
                OnSessionTerminated(slotCookie, sessionId, &entry);
            });
    }

    //! Retires a session once the controller has raised its terminal alternative.
    void OnSessionTerminated(
        int slotCookie,
        TSessionId sessionId,
        TNonNullPtr<TSessionEntry> entry)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        auto& slot = GetOrCrash(Slots_, slotCookie);
        slot.ActiveSessionIds.erase(
            std::remove(slot.ActiveSessionIds.begin(), slot.ActiveSessionIds.end(), sessionId),
            slot.ActiveSessionIds.end());

        YT_TLOG_DEBUG("Session terminated")
            .With("SlotCookie", slotCookie)
            .With("SessionId", sessionId);

        if (auto chunkId = MaybeMarkSessionSealed(entry)) {
            ScheduleChunkSeal(slotCookie, sessionId, *chunkId);
        }
    }

    void DoFinalizeSlot(int slotCookie) noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<std::pair<TSessionId, IDistributedChunkSessionControllerPtr>> activeSessions;
        std::vector<TSessionId> sessionsToSeal;
        auto& slot = Slots_[slotCookie];

        if (slot.Finalized) {
            YT_TLOG_DEBUG("Slot is already finalized")
                .With("SlotCookie", slotCookie);
            return;
        }

        slot.Finalized = true;
        auto sessionCount = std::ssize(slot.AllSessionIds);
        activeSessions.reserve(slot.ActiveSessionIds.size());

        for (const auto& sessionId : slot.ActiveSessionIds) {
            activeSessions.emplace_back(
                sessionId,
                GetOrCrash(slot.Sessions, sessionId).Controller);
        }
        slot.ActiveSessionIds.clear();

        for (const auto& sessionId : slot.AllSessionIds) {
            if (MaybeMarkSessionSealed(&GetOrCrash(slot.Sessions, sessionId))) {
                sessionsToSeal.push_back(sessionId);
            }
        }

        YT_TLOG_DEBUG("Finalizing slot")
            .With("SlotCookie", slotCookie)
            .With("SessionCount", sessionCount)
            .With("ActiveSessionCount", std::ssize(activeSessions))
            .With("SealedChunkCount", std::ssize(sessionsToSeal));

        // TODO(apollo1321): For now session close and chunk seal scheduling run in parallel.
        // Finalize through sequencer stats first and seal directly when close succeeds; keep
        // ScheduleChunkSeal as a fallback for sessions that cannot be closed cleanly.
        for (const auto& [sessionId, controller] : activeSessions) {
            YT_UNUSED_FUTURE(controller->Close());
        }

        // TODO(apollo1321): Batch chunk seal scheduling instead of issuing one request per chunk.
        for (auto sessionId : sessionsToSeal) {
            ScheduleChunkSeal(slotCookie, sessionId, sessionId.ChunkId);
        }

        YT_TLOG_DEBUG("Slot finalized")
            .With("SlotCookie", slotCookie)
            .With("SealedChunkCount", std::ssize(sessionsToSeal));
    }

    void ScheduleChunkSeal(int slotCookie, TSessionId sessionId, TChunkId chunkId)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        YT_TLOG_DEBUG("Scheduling chunk sealing")
            .With("SlotCookie", slotCookie)
            .With("SessionId", sessionId)
            .With("ChunkId", chunkId);

        // NB: A synchronous throw would escape a bare invoker post and terminate the
        // process, so it is reported as an ordinary seal failure.
        auto sealScheduled = [&] {
            try {
                return SendChunkSealRequest_(chunkId);
            } catch (const std::exception& ex) {
                return MakeFuture<void>(TError(ex));
            }
        }();

        sealScheduled
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
        const TError& error) noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (error.IsOK()) {
            auto& entry = GetOrCrash(GetOrCrash(Slots_, slotCookie).Sessions, sessionId);
            entry.SealRetryBackoff.reset();
            StartTerminalRecovery(slotCookie, sessionId, &entry);

            YT_TLOG_DEBUG("Chunk sealing scheduled")
                .With("SlotCookie", slotCookie)
                .With("SessionId", sessionId)
                .With("ChunkId", chunkId);
            return;
        }

        auto& entry = GetOrCrash(GetOrCrash(Slots_, slotCookie).Sessions, sessionId);
        auto& sealRetryBackoff = entry.SealRetryBackoff;
        if (!sealRetryBackoff) {
            sealRetryBackoff.emplace(Config_->ChunkSealRetryBackoff);
        }

        if (!sealRetryBackoff->Next()) {
            ReportTerminalSessionFailure(
                slotCookie,
                sessionId,
                &entry,
                TError("Chunk sealing failed; terminal session progress is unavailable")
                    .With(error));
            YT_TLOG_ALERT("Failed to schedule chunk sealing; retries exhausted")
                .With("SlotCookie", slotCookie)
                .With("SessionId", sessionId)
                .With("ChunkId", chunkId)
                .With("InvocationCount", sealRetryBackoff->GetInvocationCount())
                .With(error);
            return;
        }

        TDuration retryBackoff = sealRetryBackoff->GetBackoff();

        YT_TLOG_WARNING("Failed to schedule chunk sealing; retrying")
            .With("SlotCookie", slotCookie)
            .With("SessionId", sessionId)
            .With("ChunkId", chunkId)
            .With("RetryIndex", sealRetryBackoff->GetInvocationIndex())
            .With("RetryBackoff", retryBackoff)
            .With(error);

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
                .Progress = entry.Progress,
            });
        }

        return result;
    }

    std::vector<TReadySession> DoGetReadySessions() const
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<TReadySession> result;
        result.reserve(Slots_.size());
        for (const auto& [slotCookie, slot] : Slots_) {
            if (slot.Finalized || slot.ActiveSessionIds.empty()) {
                continue;
            }

            const auto& sessionId = slot.ActiveSessionIds.front();
            const auto& entry = GetOrCrash(slot.Sessions, sessionId);
            result.push_back(TReadySession{
                .SlotCookie = slotCookie,
                .Descriptor = TSessionDescriptor{
                    .SessionId = entry.StartedSession.SessionId,
                    .SequencerNode = entry.StartedSession.SequencerNode,
                },
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
    IDistributedChunkSessionSealMonitorPtr sealMonitor,
    TLogger logger)
{
    auto Logger = logger;
    auto chunkSealRpcTimeout = config->ChunkSealRpcTimeout;

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
        YT_TLOG_DEBUG("Sending chunk seal request")
            .With("ChunkId", chunkId);

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
                YT_TLOG_DEBUG("Chunk seal request succeeded")
                    .With("ChunkId", chunkId);
            } else {
                YT_TLOG_WARNING("Chunk seal request failed")
                    .With("ChunkId", chunkId)
                    .With(error);
            }
        }));

        return future;
    });

    return New<TDistributedChunkSessionPool>(
        std::move(config),
        std::move(createController),
        std::move(sendChunkSealRequest),
        std::move(invoker),
        std::move(sealMonitor),
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
        std::move(options.SealMonitor),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
