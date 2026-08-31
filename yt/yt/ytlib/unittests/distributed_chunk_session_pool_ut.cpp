#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/seal_monitor.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/session_controller.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/session_pool.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/private.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/suspendable_action_queue.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/yson_string/string.h>

#include <library/cpp/yt/misc/property.h>

#include <algorithm>

namespace NYT::NDistributedChunkSessionClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;

namespace {

////////////////////////////////////////////////////////////////////////////////

TStartedSessionInfo MakeStartedSessionInfo(
    ui64 counter,
    int mediumIndex,
    const std::string& address)
{
    auto chunkId = MakeId(EObjectType::JournalChunk, TCellTag(1), counter, 0);

    TChunkReplicaWithMediumList replicas;
    for (int index = 0; index < 3; ++index) {
        replicas.push_back(TChunkReplicaWithMedium(TNodeId(index + 1), index, mediumIndex));
    }

    return TStartedSessionInfo{
        .SessionId = TSessionId(chunkId, mediumIndex),
        .SequencerNode = TNodeDescriptor(address),
        .Replicas = std::move(replicas),
    };
}

class TFakeDistributedChunkSessionController
    : public IDistributedChunkSessionController
{
public:
    DEFINE_BYVAL_RO_PROPERTY(int, CloseCallCount);

    explicit TFakeDistributedChunkSessionController(
        TStartedSessionInfo startedSession,
        bool delayStart = false,
        bool delayClose = false,
        std::optional<TError> closeError = {})
        : StartedSession_(std::move(startedSession))
        , DelayStart_(delayStart)
        , DelayClose_(delayClose)
        , CloseError_(std::move(closeError))
    { }

    TFuture<TStartedSessionInfo> StartSession() final
    {
        return DelayStart_
            ? StartPromise_.ToFuture()
            : MakeFuture(StartedSession_);
    }

    TFuture<void> Close() final
    {
        ++CloseCallCount_;
        if (!DelayClose_) {
            FinishClose();
        }
        return ClosedPromise_.ToFuture();
    }

    TFuture<void> GetClosedFuture() final
    {
        return ClosedPromise_.ToFuture();
    }

    TSessionId GetSessionId() const final
    {
        return StartedSession_.SessionId;
    }

    DEFINE_SIGNAL_OVERRIDE(TSessionProgressUpdatedSignature, ProgressUpdated);

    void FailUnexpectedly(const TError& error)
    {
        ProgressUpdated_.Fire(TSessionCloseFailed(error));
        ClosedPromise_.TrySet(error);
    }

    void FulfillStartSession()
    {
        if (DelayStart_) {
            StartPromise_.TrySet(StartedSession_);
        }
    }

    void FulfillClose()
    {
        if (DelayClose_) {
            FinishClose();
        }
    }

    void UpdateProgress(const TDistributedChunkSessionProgress& progress)
    {
        Progress_ = progress;
        ProgressUpdated_.Fire(TSessionInFlightProgress(progress));
    }

private:
    const TStartedSessionInfo StartedSession_;
    const bool DelayStart_ = false;
    const bool DelayClose_ = false;
    const std::optional<TError> CloseError_;
    const TPromise<TStartedSessionInfo> StartPromise_ = NewPromise<TStartedSessionInfo>();
    const TPromise<void> ClosedPromise_ = NewPromise<void>();
    std::optional<TDistributedChunkSessionProgress> Progress_;

    void FinishClose()
    {
        auto error = CloseError_.value_or(TError());
        if (error.IsOK()) {
            ProgressUpdated_.Fire(TSessionFinalProgress(Progress_));
        } else {
            ProgressUpdated_.Fire(TSessionCloseFailed(error));
        }
        ClosedPromise_.TrySet(error);
    }
};

using TFakeDistributedChunkSessionControllerPtr = TIntrusivePtr<TFakeDistributedChunkSessionController>;

////////////////////////////////////////////////////////////////////////////////

class TFakeDistributedChunkSessionSealMonitor
    : public IDistributedChunkSessionSealMonitor
{
public:
    class TSubscription
        : public IDistributedChunkSessionSealSubscription
    {
    public:
        explicit TSubscription(TIntrusivePtr<TFakeDistributedChunkSessionSealMonitor> monitor)
            : Monitor_(std::move(monitor))
        { }

        void TrackChunks(std::vector<TChunkId> chunkIds) final
        {
            Monitor_->TrackChunks(std::move(chunkIds));
        }

    private:
        const TIntrusivePtr<TFakeDistributedChunkSessionSealMonitor> Monitor_;
    };

    TDistributedChunkSessionSealSubscriptionPtr Subscribe(
        TDistributedChunkSessionSealedCallback callback) final
    {
        YT_VERIFY(!Callback_);
        Callback_ = std::move(callback);
        return std::make_unique<TSubscription>(MakeStrong(this));
    }

    void Reconfigure(TDistributedChunkSessionSealMonitorConfigPtr /*config*/) final
    { }

    const std::vector<TChunkId>& GetTrackedChunkIds() const
    {
        return TrackedChunkIds_;
    }

    void DeliverSealSummary(TSessionSealSummaryWithChunkId summary)
    {
        YT_VERIFY(Callback_);
        auto it = std::ranges::find(TrackedChunkIds_, summary.ChunkId);
        YT_VERIFY(it != TrackedChunkIds_.end());
        TrackedChunkIds_.erase(it);
        Callback_(std::vector{summary});
    }

private:
    TDistributedChunkSessionSealedCallback Callback_;
    std::vector<TChunkId> TrackedChunkIds_;

    void TrackChunks(std::vector<TChunkId> chunkIds)
    {
        for (auto chunkId : chunkIds) {
            YT_VERIFY(std::ranges::find(TrackedChunkIds_, chunkId) == TrackedChunkIds_.end());
            TrackedChunkIds_.push_back(chunkId);
        }
    }
};

using TFakeDistributedChunkSessionSealMonitorPtr =
    TIntrusivePtr<TFakeDistributedChunkSessionSealMonitor>;

////////////////////////////////////////////////////////////////////////////////

class TPoolHarness
{
public:
    struct TControllerSpec
    {
        TStartedSessionInfo StartedSession;
        bool DelayStart = false;
        bool DelayClose = false;
        std::optional<TError> CloseError;
    };

    DEFINE_BYVAL_RO_PROPERTY(int, CreateControllerCallCount);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TChunkId>, ScheduledSeals);

    explicit TPoolHarness(std::vector<TStartedSessionInfo> startedSessions)
        : TPoolHarness(BuildControllerSpecs(std::move(startedSessions)))
    { }

    explicit TPoolHarness(std::vector<TControllerSpec> controllerSpecs)
        : ControllerSpecs_(std::move(controllerSpecs))
        , ActionQueue_(CreateSuspendableActionQueue("PoolTest"))
        , SealMonitor_(New<TFakeDistributedChunkSessionSealMonitor>())
    { }

    IDistributedChunkSessionPoolPtr CreatePool(int maxActiveSessionsPerSlot)
    {
        auto config = New<TDistributedChunkSessionPoolConfig>();
        config->SetDefaults();
        config->MaxActiveSessionsPerSlot = maxActiveSessionsPerSlot;
        if (HasChunkSealRetryBackoff_) {
            config->ChunkSealRetryBackoff = ChunkSealRetryBackoff_;
        }

        return CreateDistributedChunkSessionPoolForTesting(
            config,
            TDistributedChunkSessionPoolTestingOptions{
                .CreateController = BIND([this] {
                    return CreateController();
                }),
                .SendChunkSealRequest = BIND([this] (TChunkId chunkId) {
                    ScheduledSeals_.push_back(chunkId);

                    int callIndex = ScheduleChunkSealCallCount_++;
                    if (callIndex < ScheduleChunkSealThrowCount_) {
                        THROW_ERROR_EXCEPTION("Injected synchronous chunk seal failure");
                    }

                    if (callIndex < std::ssize(ScheduleChunkSealErrors_)) {
                        return MakeFuture(ScheduleChunkSealErrors_[callIndex]);
                    }

                    return MakeFuture(TError());
                }),
                .SealMonitor = SealMonitor_,
            },
            ActionQueue_->GetInvoker());
    }

    const std::vector<TStartedSessionInfo>& StartedSessions() const
    {
        return StartedSessions_;
    }

    TFakeDistributedChunkSessionControllerPtr GetController(TSessionId sessionId) const
    {
        return GetOrCrash(Controllers_, sessionId);
    }

    void SetChunkSealRetryBackoff(TExponentialBackoffOptions chunkSealRetryBackoff)
    {
        ChunkSealRetryBackoff_ = std::move(chunkSealRetryBackoff);
        HasChunkSealRetryBackoff_ = true;
    }

    void SetScheduleChunkSealErrors(std::vector<TError> scheduleChunkSealErrors)
    {
        ScheduleChunkSealErrors_ = std::move(scheduleChunkSealErrors);
    }

    void SetScheduleChunkSealThrowCount(int scheduleChunkSealThrowCount)
    {
        ScheduleChunkSealThrowCount_ = scheduleChunkSealThrowCount;
    }

    void DrainInvoker()
    {
        WaitFor(ActionQueue_->Suspend(/*immediately*/ false))
            .ThrowOnError();
        ActionQueue_->Resume();
    }

    const std::vector<TChunkId>& GetTrackedChunkIds() const
    {
        return SealMonitor_->GetTrackedChunkIds();
    }

    void DeliverSealSummary(TSessionSealSummaryWithChunkId summary)
    {
        ActionQueue_->GetInvoker()->Invoke(BIND(
            &TFakeDistributedChunkSessionSealMonitor::DeliverSealSummary,
            SealMonitor_,
            summary));
        DrainInvoker();
    }

private:
    const std::vector<TControllerSpec> ControllerSpecs_;
    const std::vector<TStartedSessionInfo> StartedSessions_ = [] (const std::vector<TControllerSpec>& controllerSpecs) {
        std::vector<TStartedSessionInfo> startedSessions;
        startedSessions.reserve(controllerSpecs.size());

        for (const auto& spec : controllerSpecs) {
            startedSessions.push_back(spec.StartedSession);
        }

        return startedSessions;
    }(ControllerSpecs_);
    const ISuspendableActionQueuePtr ActionQueue_;
    const TFakeDistributedChunkSessionSealMonitorPtr SealMonitor_;

    THashMap<TSessionId, TFakeDistributedChunkSessionControllerPtr> Controllers_;
    TExponentialBackoffOptions ChunkSealRetryBackoff_;
    bool HasChunkSealRetryBackoff_ = false;
    std::vector<TError> ScheduleChunkSealErrors_;
    int ScheduleChunkSealThrowCount_ = 0;
    int ScheduleChunkSealCallCount_ = 0;

    static std::vector<TControllerSpec> BuildControllerSpecs(std::vector<TStartedSessionInfo> startedSessions)
    {
        std::vector<TControllerSpec> controllerSpecs;
        controllerSpecs.reserve(startedSessions.size());

        for (auto& startedSession : startedSessions) {
            controllerSpecs.push_back(TControllerSpec{
                .StartedSession = std::move(startedSession),
            });
        }

        return controllerSpecs;
    }

    IDistributedChunkSessionControllerPtr CreateController()
    {
        EXPECT_LT(CreateControllerCallCount_, std::ssize(ControllerSpecs_));

        const auto& spec = ControllerSpecs_[CreateControllerCallCount_++];
        auto controller = New<TFakeDistributedChunkSessionController>(
            spec.StartedSession,
            spec.DelayStart,
            spec.DelayClose,
            spec.CloseError);
        EmplaceOrCrash(Controllers_, controller->GetSessionId(), controller);
        return controller;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TDistributedChunkSessionPoolTest, CreatesFirstSessionForEmptySlot)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);

    auto session = WaitFor(pool->GetSession(17))
        .ValueOrThrow();

    EXPECT_EQ(session.SessionId, harness.StartedSessions()[0].SessionId);
    EXPECT_EQ(
        session.SequencerNode.GetDefaultAddress(),
        harness.StartedSessions()[0].SequencerNode.GetDefaultAddress());
    EXPECT_EQ(harness.GetCreateControllerCallCount(), 1);
}

TEST(TDistributedChunkSessionPoolTest, ReportsAndRetainsSessionProgress)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto updatePromise = NewPromise<TSessionProgressUpdate>();
    pool->SubscribeProgressUpdated(BIND(
        [updatePromise] (const TSessionProgressUpdate& update) {
            if (std::holds_alternative<TSessionInFlightProgress>(update.Progress)) {
                updatePromise.TrySet(update);
            }
        }));

    auto session = WaitFor(pool->GetSession(17))
        .ValueOrThrow();
    auto progress = TDistributedChunkSessionProgress{
        .DataWeight = 11,
        .CompressedDataSize = 13,
        .UncompressedDataSize = 17,
        .RecordCount = 2,
        .RowCount = 5,
    };

    harness.GetController(session.SessionId)->UpdateProgress(progress);

    auto update = WaitFor(
        updatePromise.ToFuture().WithTimeout(TDuration::Seconds(5)))
        .ValueOrThrow();
    EXPECT_EQ(update.SlotCookie, 17);
    EXPECT_EQ(update.SessionId, session.SessionId);
    EXPECT_EQ(
        std::get<TSessionInFlightProgress>(update.Progress).Underlying(),
        progress);

    pool->FinalizeSlot(17);
    harness.DrainInvoker();

    auto chunks = WaitFor(pool->GetSlotChunks(17))
        .ValueOrThrow();
    ASSERT_EQ(chunks.size(), 1u);
    EXPECT_EQ(chunks[0].ChunkId, session.SessionId.ChunkId);
    ASSERT_TRUE(chunks[0].Progress.has_value());
    EXPECT_EQ(*chunks[0].Progress, progress);
}

TEST(TDistributedChunkSessionPoolTest, PreservesMissingSessionProgress)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    WaitFor(pool->GetSession(17))
        .ThrowOnError();

    auto chunks = WaitFor(pool->GetSlotChunks(17))
        .ValueOrThrow();
    ASSERT_EQ(chunks.size(), 1u);
    EXPECT_FALSE(chunks[0].Progress.has_value());
}

TEST(TDistributedChunkSessionPoolTest, ReportsMasterSealResultWithConfirmedProgress)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto sealedPromise = NewPromise<TSessionProgressUpdate>();
    pool->SubscribeProgressUpdated(BIND(
        [sealedPromise] (const TSessionProgressUpdate& update) {
            if (std::holds_alternative<TSessionSealSummary>(update.Progress)) {
                sealedPromise.TrySet(update);
            }
        }));
    auto session = WaitFor(pool->GetSession(17))
        .ValueOrThrow();
    auto progress = TDistributedChunkSessionProgress{
        .DataWeight = 100,
        .CompressedDataSize = 50,
        .UncompressedDataSize = 200,
        .RecordCount = 2,
        .RowCount = 10,
    };
    harness.GetController(session.SessionId)->UpdateProgress(progress);
    harness.GetController(session.SessionId)->FailUnexpectedly(TError("boom"));
    harness.DrainInvoker();

    EXPECT_THAT(harness.GetTrackedChunkIds(), ::testing::ElementsAre(session.SessionId.ChunkId));

    harness.DeliverSealSummary({
        .ChunkId = session.SessionId.ChunkId,
        .Summary = {
            .RecordCount = 5,
            .PhysicalCompressedDataSize = 999,
        },
    });

    auto update = WaitFor(
        sealedPromise.ToFuture().WithTimeout(TDuration::Seconds(5)))
        .ValueOrThrow();
    EXPECT_EQ(update.SlotCookie, 17);
    EXPECT_EQ(update.SessionId, session.SessionId);
    EXPECT_EQ(
        std::get<TSessionSealSummary>(update.Progress),
        (TSessionSealSummary{.RecordCount = 5, .PhysicalCompressedDataSize = 999}));

    auto chunks = WaitFor(pool->GetSlotChunks(17))
        .ValueOrThrow();
    ASSERT_EQ(chunks.size(), 1u);
    ASSERT_TRUE(chunks[0].Progress.has_value());
    EXPECT_EQ(*chunks[0].Progress, progress);
}

//! Slot finalization runs on a bare invoker post, so a chunk seal request that fails
//! synchronously has nowhere to be delivered and must not escape as an exception.
TEST(TDistributedChunkSessionPoolTest, ThrowingChunkSealRequestDoesNotEscapeSlotFinalization)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });
    harness.SetChunkSealRetryBackoff(TExponentialBackoffOptions{
        .InvocationCount = 1,
        .MinBackoff = TDuration::MilliSeconds(1),
        .MaxBackoff = TDuration::MilliSeconds(1),
        .BackoffMultiplier = 1.0,
        .BackoffJitter = 0.0,
    });
    harness.SetScheduleChunkSealThrowCount(5);

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    std::vector<TSessionProgressUpdate> updates;
    pool->SubscribeProgressUpdated(BIND(
        [&] (const TSessionProgressUpdate& update) {
            updates.push_back(update);
        }));

    auto session = WaitFor(pool->GetSession(11))
        .ValueOrThrow();
    auto progress = TDistributedChunkSessionProgress{
        .DataWeight = 100,
        .CompressedDataSize = 50,
        .UncompressedDataSize = 200,
        .RecordCount = 2,
        .RowCount = 10,
    };
    harness.GetController(session.SessionId)->UpdateProgress(progress);

    pool->FinalizeSlot(11);
    harness.DrainInvoker();

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(50));
    harness.DrainInvoker();

    // NB: The clean close still publishes the exact terminal result; the failed seal
    // scheduling only exhausts its retries in the background.
    ASSERT_EQ(updates.size(), 2u);
    EXPECT_EQ(std::get<TSessionInFlightProgress>(updates[0].Progress).Underlying(), progress);
    ASSERT_TRUE(std::get<TSessionFinalProgress>(updates[1].Progress).Underlying().has_value());
    EXPECT_EQ(*std::get<TSessionFinalProgress>(updates[1].Progress).Underlying(), progress);
}

//! Slot finalization schedules chunk sealing in parallel with the session close, so a
//! seal summary may arrive after the clean close has already published exact progress.
TEST(TDistributedChunkSessionPoolTest, SealSummaryAfterFinalProgressIsIgnored)
{
    TPoolHarness harness({
        TPoolHarness::TControllerSpec{
            .StartedSession = MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
            .DelayClose = true,
        },
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    std::vector<TSessionProgressUpdate> updates;
    pool->SubscribeProgressUpdated(BIND(
        [&] (const TSessionProgressUpdate& update) {
            updates.push_back(update);
        }));

    auto session = WaitFor(pool->GetSession(17))
        .ValueOrThrow();
    auto progress = TDistributedChunkSessionProgress{
        .DataWeight = 100,
        .CompressedDataSize = 50,
        .UncompressedDataSize = 200,
        .RecordCount = 2,
        .RowCount = 10,
    };
    harness.GetController(session.SessionId)->UpdateProgress(progress);

    // Master acknowledges the seal while the session close is still in flight.
    pool->FinalizeSlot(17);
    harness.DrainInvoker();
    EXPECT_THAT(harness.GetTrackedChunkIds(), ::testing::ElementsAre(session.SessionId.ChunkId));

    harness.GetController(session.SessionId)->FulfillClose();
    harness.DrainInvoker();

    harness.DeliverSealSummary({
        .ChunkId = session.SessionId.ChunkId,
        .Summary = {
            .RecordCount = 5,
            .PhysicalCompressedDataSize = 999,
        },
    });

    ASSERT_EQ(updates.size(), 2u);
    EXPECT_EQ(std::get<TSessionInFlightProgress>(updates[0].Progress).Underlying(), progress);
    ASSERT_TRUE(std::get<TSessionFinalProgress>(updates[1].Progress).Underlying().has_value());
    EXPECT_EQ(*std::get<TSessionFinalProgress>(updates[1].Progress).Underlying(), progress);
}

TEST(TDistributedChunkSessionPoolTest, ReportsMasterSealResultWithoutConfirmedProgress)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto sealedPromise = NewPromise<TSessionProgressUpdate>();
    pool->SubscribeProgressUpdated(BIND(
        [sealedPromise] (const TSessionProgressUpdate& update) {
            if (std::holds_alternative<TSessionSealSummary>(update.Progress)) {
                sealedPromise.TrySet(update);
            }
        }));
    auto session = WaitFor(pool->GetSession(17))
        .ValueOrThrow();
    harness.GetController(session.SessionId)->FailUnexpectedly(TError("boom"));
    harness.DrainInvoker();

    harness.DeliverSealSummary({
        .ChunkId = session.SessionId.ChunkId,
        .Summary = {
            .RecordCount = 2,
            .PhysicalCompressedDataSize = 100,
        },
    });

    auto update = WaitFor(
        sealedPromise.ToFuture().WithTimeout(TDuration::Seconds(5)))
        .ValueOrThrow();
    EXPECT_EQ(update.SlotCookie, 17);
    EXPECT_EQ(update.SessionId, session.SessionId);
    EXPECT_EQ(
        std::get<TSessionSealSummary>(update.Progress),
        (TSessionSealSummary{.RecordCount = 2, .PhysicalCompressedDataSize = 100}));

    auto chunks = WaitFor(pool->GetSlotChunks(17))
        .ValueOrThrow();
    ASSERT_EQ(chunks.size(), 1u);
    EXPECT_FALSE(chunks[0].Progress.has_value());
}

TEST(TDistributedChunkSessionPoolTest, IgnoresProgressReportedAfterSessionFinished)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto session = WaitFor(pool->GetSession(17))
        .ValueOrThrow();
    harness.GetController(session.SessionId)->FailUnexpectedly(TError("boom"));
    harness.DrainInvoker();
    harness.DeliverSealSummary({
        .ChunkId = session.SessionId.ChunkId,
        .Summary = {
            .RecordCount = 5,
            .PhysicalCompressedDataSize = 100,
        },
    });

    harness.GetController(session.SessionId)->UpdateProgress({
        .DataWeight = 40,
        .CompressedDataSize = 20,
        .UncompressedDataSize = 80,
        .RecordCount = 2,
        .RowCount = 10,
    });
    harness.DrainInvoker();

    auto chunks = WaitFor(pool->GetSlotChunks(17))
        .ValueOrThrow();
    ASSERT_EQ(chunks.size(), 1u);
    EXPECT_FALSE(chunks[0].Progress.has_value());
}

TEST(TDistributedChunkSessionPoolTest, ReportsFinalProgressAfterCleanClose)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto session = WaitFor(pool->GetSession(17))
        .ValueOrThrow();
    auto progress = TDistributedChunkSessionProgress{
        .DataWeight = 100,
        .CompressedDataSize = 50,
        .UncompressedDataSize = 200,
        .RecordCount = 2,
        .RowCount = 10,
    };
    harness.GetController(session.SessionId)->UpdateProgress(progress);
    harness.DrainInvoker();

    std::vector<TSessionProgressUpdate> updates;
    pool->SubscribeProgressUpdated(BIND(
        [&] (const TSessionProgressUpdate& update) {
            updates.push_back(update);
        }));

    pool->FinalizeSlot(17);
    harness.DrainInvoker();

    ASSERT_EQ(updates.size(), 1u);
    const auto& update = updates.front();
    EXPECT_EQ(update.SlotCookie, 17);
    EXPECT_EQ(update.SessionId, session.SessionId);
    EXPECT_EQ(
        std::get<TSessionFinalProgress>(update.Progress).Underlying(),
        progress);
}

TEST(TDistributedChunkSessionPoolTest, ReportsFinalProgressWhenCloseIsCalledOutsideFinalize)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto session = WaitFor(pool->GetSession(17))
        .ValueOrThrow();
    auto progress = TDistributedChunkSessionProgress{
        .DataWeight = 100,
        .CompressedDataSize = 50,
        .UncompressedDataSize = 200,
        .RecordCount = 2,
        .RowCount = 10,
    };
    harness.GetController(session.SessionId)->UpdateProgress(progress);
    harness.DrainInvoker();

    std::vector<TSessionProgressUpdate> updates;
    pool->SubscribeProgressUpdated(BIND(
        [&] (const TSessionProgressUpdate& update) {
            updates.push_back(update);
        }));

    // NB: Nobody but the caller holds this future; the pool must still learn the outcome.
    YT_UNUSED_FUTURE(harness.GetController(session.SessionId)->Close());
    harness.DrainInvoker();

    ASSERT_EQ(updates.size(), 1u);
    EXPECT_EQ(
        std::get<TSessionFinalProgress>(updates.front().Progress).Underlying(),
        progress);
    EXPECT_TRUE(harness.GetTrackedChunkIds().empty());
}

TEST(TDistributedChunkSessionPoolTest, ReportsCloseFailureWhenCloseIsCalledOutsideFinalize)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto session = WaitFor(pool->GetSession(17))
        .ValueOrThrow();

    std::vector<TSessionProgressUpdate> updates;
    pool->SubscribeProgressUpdated(BIND(
        [&] (const TSessionProgressUpdate& update) {
            updates.push_back(update);
        }));

    harness.GetController(session.SessionId)->FailUnexpectedly(TError("boom"));
    harness.DrainInvoker();

    EXPECT_TRUE(updates.empty());
    EXPECT_THAT(harness.GetTrackedChunkIds(), ::testing::ElementsAre(session.SessionId.ChunkId));

    harness.DeliverSealSummary({
        .ChunkId = session.SessionId.ChunkId,
        .Summary = {.RecordCount = 7, .PhysicalCompressedDataSize = 70},
    });

    ASSERT_EQ(updates.size(), 1u);
    EXPECT_EQ(
        std::get<TSessionSealSummary>(updates.front().Progress),
        (TSessionSealSummary{.RecordCount = 7, .PhysicalCompressedDataSize = 70}));
}

TEST(TDistributedChunkSessionPoolTest, DoesNotTrackCleanCloseWithFinalProgress)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto session = WaitFor(pool->GetSession(17))
        .ValueOrThrow();
    harness.GetController(session.SessionId)->UpdateProgress({
        .DataWeight = 100,
        .CompressedDataSize = 50,
        .UncompressedDataSize = 200,
        .RecordCount = 2,
        .RowCount = 10,
    });

    pool->FinalizeSlot(17);
    harness.DrainInvoker();

    EXPECT_TRUE(harness.GetTrackedChunkIds().empty());
}

TEST(TDistributedChunkSessionPoolTest, TracksCleanCloseWithoutFinalProgress)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto session = WaitFor(pool->GetSession(17))
        .ValueOrThrow();

    pool->FinalizeSlot(17);
    harness.DrainInvoker();

    EXPECT_THAT(harness.GetTrackedChunkIds(), ::testing::ElementsAre(session.SessionId.ChunkId));
}

TEST(TDistributedChunkSessionPoolTest, ReusesExistingActiveSession)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);

    auto first = WaitFor(pool->GetSession(17))
        .ValueOrThrow();
    auto second = WaitFor(pool->GetSession(17))
        .ValueOrThrow();

    EXPECT_EQ(second.SessionId, first.SessionId);
    EXPECT_EQ(harness.GetCreateControllerCallCount(), 1);
}

TEST(TDistributedChunkSessionPoolTest, PicksDifferentActiveSessions)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
        MakeStartedSessionInfo(/*counter*/ 2, /*mediumIndex*/ 0, "node-2"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);

    auto first = WaitFor(pool->GetSession(17))
        .ValueOrThrow();
    auto second = WaitFor(pool->GetSession(17, first.SessionId))
        .ValueOrThrow();

    bool sawFirst = false;
    bool sawSecond = false;

    for (int index = 0; index < 100; ++index) {
        auto picked = WaitFor(pool->GetSession(17))
            .ValueOrThrow();
        sawFirst |= picked.SessionId == first.SessionId;
        sawSecond |= picked.SessionId == second.SessionId;
    }

    EXPECT_TRUE(sawFirst);
    EXPECT_TRUE(sawSecond);
}

TEST(TDistributedChunkSessionPoolTest, RetryCreatesNewSessionUnderCap)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
        MakeStartedSessionInfo(/*counter*/ 2, /*mediumIndex*/ 0, "node-2"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);

    auto first = WaitFor(pool->GetSession(5))
        .ValueOrThrow();
    auto second = WaitFor(pool->GetSession(5, first.SessionId))
        .ValueOrThrow();

    EXPECT_NE(second.SessionId, first.SessionId);
    EXPECT_EQ(harness.GetCreateControllerCallCount(), 2);
}

TEST(TDistributedChunkSessionPoolTest, RetryReturnsDifferentSessionAtCap)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
        MakeStartedSessionInfo(/*counter*/ 2, /*mediumIndex*/ 0, "node-2"),
        MakeStartedSessionInfo(/*counter*/ 3, /*mediumIndex*/ 0, "node-3"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 2);

    auto first = WaitFor(pool->GetSession(5))
        .ValueOrThrow();
    auto second = WaitFor(pool->GetSession(5, first.SessionId))
        .ValueOrThrow();
    auto third = WaitFor(pool->GetSession(5, first.SessionId))
        .ValueOrThrow();

    EXPECT_EQ(harness.GetCreateControllerCallCount(), 2);
    EXPECT_EQ(third.SessionId, second.SessionId);
}

TEST(TDistributedChunkSessionPoolTest, RetryReturnsSameSessionWhenCapIsOne)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
        MakeStartedSessionInfo(/*counter*/ 2, /*mediumIndex*/ 0, "node-2"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 1);

    auto first = WaitFor(pool->GetSession(5))
        .ValueOrThrow();
    auto second = WaitFor(pool->GetSession(5, first.SessionId))
        .ValueOrThrow();
    auto third = WaitFor(pool->GetSession(5, first.SessionId))
        .ValueOrThrow();

    EXPECT_EQ(harness.GetCreateControllerCallCount(), 1);
    EXPECT_EQ(second.SessionId, first.SessionId);
    EXPECT_EQ(third.SessionId, first.SessionId);
}

TEST(TDistributedChunkSessionPoolTest, ConcurrentRetriesDoNotExceedCap)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
        MakeStartedSessionInfo(/*counter*/ 2, /*mediumIndex*/ 0, "node-2"),
        MakeStartedSessionInfo(/*counter*/ 3, /*mediumIndex*/ 0, "node-3"),
        MakeStartedSessionInfo(/*counter*/ 4, /*mediumIndex*/ 0, "node-4"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto first = WaitFor(pool->GetSession(5))
        .ValueOrThrow();

    std::vector<TFuture<TSessionDescriptor>> futures;
    for (int index = 0; index < 8; ++index) {
        futures.push_back(pool->GetSession(5, first.SessionId));
    }

    WaitFor(AllSucceeded(futures))
        .ThrowOnError();
    EXPECT_EQ(harness.GetCreateControllerCallCount(), 3);
}

TEST(TDistributedChunkSessionPoolTest, CancelledWaiterDoesNotPoisonPendingSessionCreation)
{
    TPoolHarness harness(std::vector<TPoolHarness::TControllerSpec>{
        {
            .StartedSession = MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
            .DelayStart = true,
        },
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);

    auto future = pool->GetSession(19);
    future.Cancel(TError("cancel"));

    harness.DrainInvoker();
    ASSERT_EQ(harness.GetCreateControllerCallCount(), 1);
    harness.GetController(harness.StartedSessions()[0].SessionId)->FulfillStartSession();
    harness.DrainInvoker();

    auto sessionOrError = WaitFor(pool->GetSession(19));

    EXPECT_TRUE(sessionOrError.IsOK());
    auto session = sessionOrError
        .ValueOrThrow();
    EXPECT_EQ(session.SessionId, harness.StartedSessions()[0].SessionId);
    EXPECT_EQ(harness.GetCreateControllerCallCount(), 1);
}

TEST(TDistributedChunkSessionPoolTest, UnexpectedCloseRemovesSessionAndSchedulesSeal)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto session = WaitFor(pool->GetSession(11))
        .ValueOrThrow();

    harness.GetController(session.SessionId)->FailUnexpectedly(TError("boom"));
    harness.DrainInvoker();

    EXPECT_THAT(harness.ScheduledSeals(), ::testing::ElementsAre(session.SessionId.ChunkId));
}

TEST(TDistributedChunkSessionPoolTest, RetriesChunkSealingAfterFailure)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });
    harness.SetChunkSealRetryBackoff(TExponentialBackoffOptions{
        .InvocationCount = 10,
        .MinBackoff = TDuration::MilliSeconds(1),
        .MaxBackoff = TDuration::MilliSeconds(1),
        .BackoffMultiplier = 1.0,
        .BackoffJitter = 0.0,
    });
    harness.SetScheduleChunkSealErrors({TError("transient failure")});

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto session = WaitFor(pool->GetSession(11))
        .ValueOrThrow();

    harness.GetController(session.SessionId)->FailUnexpectedly(TError("boom"));
    harness.DrainInvoker();

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(50));
    harness.DrainInvoker();

    EXPECT_THAT(
        harness.ScheduledSeals(),
        ::testing::ElementsAre(session.SessionId.ChunkId, session.SessionId.ChunkId));
}

TEST(TDistributedChunkSessionPoolTest, ExhaustedChunkSealRetriesDoNotAbort)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });
    harness.SetChunkSealRetryBackoff(TExponentialBackoffOptions{
        .InvocationCount = 1,
        .MinBackoff = TDuration::MilliSeconds(1),
        .MaxBackoff = TDuration::MilliSeconds(1),
        .BackoffMultiplier = 1.0,
        .BackoffJitter = 0.0,
    });
    harness.SetScheduleChunkSealErrors({
        TError("first transient failure"),
        TError("second transient failure"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto session = WaitFor(pool->GetSession(11))
        .ValueOrThrow();

    harness.GetController(session.SessionId)->FailUnexpectedly(TError("boom"));
    harness.DrainInvoker();

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(50));
    harness.DrainInvoker();

    EXPECT_THAT(
        harness.ScheduledSeals(),
        ::testing::ElementsAre(session.SessionId.ChunkId, session.SessionId.ChunkId));
}

TEST(TDistributedChunkSessionPoolTest, ExhaustedChunkSealRetriesReportCloseFailure)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });
    harness.SetChunkSealRetryBackoff(TExponentialBackoffOptions{
        .InvocationCount = 1,
        .MinBackoff = TDuration::MilliSeconds(1),
        .MaxBackoff = TDuration::MilliSeconds(1),
        .BackoffMultiplier = 1.0,
        .BackoffJitter = 0.0,
    });
    harness.SetScheduleChunkSealErrors({
        TError("first transient failure"),
        TError("second transient failure"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    std::vector<TSessionProgressUpdate> updates;
    pool->SubscribeProgressUpdated(BIND(
        [&] (const TSessionProgressUpdate& update) {
            updates.push_back(update);
        }));

    auto session = WaitFor(pool->GetSession(11))
        .ValueOrThrow();
    harness.GetController(session.SessionId)->FailUnexpectedly(TError("boom"));
    harness.DrainInvoker();

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(50));
    harness.DrainInvoker();

    // NB: The chunk never seals, so the terminal alternative has to come from the pool.
    ASSERT_EQ(updates.size(), 1u);
    EXPECT_EQ(updates.front().SessionId, session.SessionId);
    EXPECT_FALSE(std::get<TSessionCloseFailed>(updates.front().Progress).Underlying().IsOK());
}

TEST(TDistributedChunkSessionPoolTest, PendingChunkSealRetryDoesNotKeepPoolAlive)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });
    harness.SetChunkSealRetryBackoff(TExponentialBackoffOptions{
        .InvocationCount = 10,
        .MinBackoff = TDuration::Seconds(1),
        .MaxBackoff = TDuration::Seconds(1),
        .BackoffMultiplier = 1.0,
        .BackoffJitter = 0.0,
    });
    harness.SetScheduleChunkSealErrors({TError("transient failure")});

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto poolWeakPtr = TWeakPtr(pool);
    auto session = WaitFor(pool->GetSession(11))
        .ValueOrThrow();

    harness.GetController(session.SessionId)->FailUnexpectedly(TError("boom"));
    harness.DrainInvoker();

    pool.Reset();
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(50));

    EXPECT_TRUE(poolWeakPtr.IsExpired());
}

TEST(TDistributedChunkSessionPoolTest, FinalizeSlotClosesLateStartedPendingSession)
{
    TPoolHarness harness(std::vector<TPoolHarness::TControllerSpec>{
        {
            .StartedSession = MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
            .DelayStart = true,
        },
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto sessionFuture = pool->GetSession(11);
    harness.DrainInvoker();

    ASSERT_EQ(harness.GetCreateControllerCallCount(), 1);
    auto sessionId = harness.StartedSessions()[0].SessionId;

    pool->FinalizeSlot(11);
    harness.GetController(sessionId)->FulfillStartSession();
    harness.DrainInvoker();

    auto sessionOrError = WaitFor(sessionFuture);
    EXPECT_FALSE(sessionOrError.IsOK());
    EXPECT_EQ(harness.GetController(sessionId)->GetCloseCallCount(), 1);

    auto scheduledSeals = harness.ScheduledSeals();
    EXPECT_THAT(scheduledSeals, ::testing::ElementsAre(sessionId.ChunkId));
}

TEST(TDistributedChunkSessionPoolTest, FinalizeSlotClosesAndSealsAllSessions)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
        MakeStartedSessionInfo(/*counter*/ 2, /*mediumIndex*/ 0, "node-2"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto first = WaitFor(pool->GetSession(11))
        .ValueOrThrow();
    auto second = WaitFor(pool->GetSession(11, first.SessionId))
        .ValueOrThrow();

    pool->FinalizeSlot(11);
    harness.DrainInvoker();

    EXPECT_EQ(harness.GetController(first.SessionId)->GetCloseCallCount(), 1);
    EXPECT_EQ(harness.GetController(second.SessionId)->GetCloseCallCount(), 1);
    EXPECT_THAT(
        harness.ScheduledSeals(),
        ::testing::UnorderedElementsAre(first.SessionId.ChunkId, second.SessionId.ChunkId));
}

TEST(TDistributedChunkSessionPoolTest, FinalizeSlotStartsSealingWithoutWaitingForLongClose)
{
    TPoolHarness harness(std::vector<TPoolHarness::TControllerSpec>{
        {
            .StartedSession = MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
            .CloseError = TError("close failed"),
        },
        {
            .StartedSession = MakeStartedSessionInfo(/*counter*/ 2, /*mediumIndex*/ 0, "node-2"),
            .DelayClose = true,
        },
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto first = WaitFor(pool->GetSession(11))
        .ValueOrThrow();
    auto second = WaitFor(pool->GetSession(11, first.SessionId))
        .ValueOrThrow();

    pool->FinalizeSlot(11);
    harness.DrainInvoker();

    EXPECT_THAT(
        harness.ScheduledSeals(),
        ::testing::UnorderedElementsAre(first.SessionId.ChunkId, second.SessionId.ChunkId));

    harness.GetController(second.SessionId)->FulfillClose();
    harness.DrainInvoker();
    EXPECT_THAT(
        harness.ScheduledSeals(),
        ::testing::UnorderedElementsAre(first.SessionId.ChunkId, second.SessionId.ChunkId));
}

TEST(TDistributedChunkSessionPoolTest, GetSlotChunksReturnsAllCreatedChunks)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
        MakeStartedSessionInfo(/*counter*/ 2, /*mediumIndex*/ 0, "node-2"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    auto first = WaitFor(pool->GetSession(11))
        .ValueOrThrow();
    auto second = WaitFor(pool->GetSession(11, first.SessionId))
        .ValueOrThrow();

    auto chunks = WaitFor(pool->GetSlotChunks(11))
        .ValueOrThrow();

    EXPECT_EQ(chunks.size(), 2u);
    EXPECT_EQ(chunks[0].Replicas.size(), 3u);
    EXPECT_EQ(chunks[1].Replicas.size(), 3u);
}

TEST(TDistributedChunkSessionPoolTest, FinalizedSlotRejectsNewSessions)
{
    TPoolHarness harness({
        MakeStartedSessionInfo(/*counter*/ 1, /*mediumIndex*/ 0, "node-1"),
    });

    auto pool = harness.CreatePool(/*maxActiveSessionsPerSlot*/ 3);
    WaitFor(pool->GetSession(11))
        .ThrowOnError();
    pool->FinalizeSlot(11);

    auto error = WaitFor(pool->GetSession(11));
    EXPECT_FALSE(error.IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDistributedChunkSessionClient
