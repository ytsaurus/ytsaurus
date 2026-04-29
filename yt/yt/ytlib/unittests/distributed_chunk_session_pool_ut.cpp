#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_controller.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_pool.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/private.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/suspendable_action_queue.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/yson_string/string.h>

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

////////////////////////////////////////////////////////////////////////////////

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
            ClosedPromise_.TrySet(CloseError_.value_or(TError()));
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

    void FailUnexpectedly(const TError& error)
    {
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
            ClosedPromise_.TrySet(CloseError_.value_or(TError()));
        }
    }

private:
    const TStartedSessionInfo StartedSession_;
    const bool DelayStart_ = false;
    const bool DelayClose_ = false;
    const std::optional<TError> CloseError_;
    const TPromise<TStartedSessionInfo> StartPromise_ = NewPromise<TStartedSessionInfo>();
    const TPromise<void> ClosedPromise_ = NewPromise<void>();
};

using TFakeDistributedChunkSessionControllerPtr = TIntrusivePtr<TFakeDistributedChunkSessionController>;

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
                    if (callIndex < std::ssize(ScheduleChunkSealErrors_)) {
                        return MakeFuture(ScheduleChunkSealErrors_[callIndex]);
                    }

                    return MakeFuture(TError());
                }),
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

    void DrainInvoker()
    {
        WaitFor(ActionQueue_->Suspend(/*immediately*/ false))
            .ThrowOnError();
        ActionQueue_->Resume();
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

    THashMap<TSessionId, TFakeDistributedChunkSessionControllerPtr> Controllers_;
    TExponentialBackoffOptions ChunkSealRetryBackoff_;
    bool HasChunkSealRetryBackoff_ = false;
    std::vector<TError> ScheduleChunkSealErrors_;
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

    WaitFor(pool->FinalizeSlot(11))
        .ThrowOnError();
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

    WaitFor(pool->FinalizeSlot(11))
        .ThrowOnError();

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

    auto finalizeFuture = pool->FinalizeSlot(11);
    harness.DrainInvoker();

    EXPECT_TRUE(finalizeFuture.IsSet());
    EXPECT_TRUE(WaitFor(finalizeFuture).IsOK());
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
    WaitFor(pool->FinalizeSlot(11))
        .ThrowOnError();

    auto error = WaitFor(pool->GetSession(11));
    EXPECT_FALSE(error.IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDistributedChunkSessionClient
