#include <yt/yt/ytlib/test_framework/test_connection.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/service_proxy.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/session_controller.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/statistics.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/proto/session_service.pb.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_service.pb.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/test_proxy_service.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NDistributedChunkSessionClient {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NThreading;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

constexpr int ReplicaCount = 3;
constexpr auto TestCellTag = TCellTag(0xf003);

const NLogging::TLogger TestLogger("DistributedChunkSessionControllerTest");

////////////////////////////////////////////////////////////////////////////////

//! Serves the master calls the controller makes while starting a session.
class TFakeMasterService
    : public TServiceBase
{
public:
    TFakeMasterService(
        IInvokerPtr invoker,
        TChunkId chunkId,
        TNodeDirectoryPtr nodeDirectory)
        : TServiceBase(
            std::move(invoker),
            TChunkServiceProxy::GetDescriptor(),
            TestLogger)
        , ChunkId_(chunkId)
        , NodeDirectory_(std::move(nodeDirectory))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AllocateWriteTargets));
    }

    void FailChunkCreation()
    {
        ChunkCreationFailed_.store(true);
    }

private:
    const TChunkId ChunkId_;
    const TNodeDirectoryPtr NodeDirectory_;

    std::atomic<bool> ChunkCreationFailed_ = false;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, CreateChunk)
    {
        if (ChunkCreationFailed_.load()) {
            context->Reply(TError("Injected chunk creation failure"));
            return;
        }

        ToProto(response->mutable_session_id(), TSessionId(ChunkId_, GenericMediumIndex));
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, AllocateWriteTargets)
    {
        NodeDirectory_->DumpTo(response->mutable_node_directory());

        auto* subresponse = response->add_subresponses();
        for (int index = 0; index < ReplicaCount; ++index) {
            subresponse->add_replicas(ToProto<ui64>(TChunkReplicaWithMedium(
                TNodeId(index),
                GenericChunkReplicaIndex,
                GenericMediumIndex)));
        }
        context->Reply();
    }
};

DEFINE_REFCOUNTED_TYPE(TFakeMasterService)

using TFakeMasterServicePtr = TIntrusivePtr<TFakeMasterService>;

////////////////////////////////////////////////////////////////////////////////

//! A sequencer whose reported progress and ping outcome the test controls.
class TFakeSequencerService
    : public TServiceBase
{
public:
    explicit TFakeSequencerService(IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            TDistributedChunkSessionServiceProxy::GetDescriptor(),
            TestLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishSession));
    }

    //! Emulates a pre-26.2 sequencer, which populates no progress at all.
    void SuppressProgress()
    {
        auto guard = Guard(Lock_);
        ProgressSuppressed_ = true;
    }

    void SetProgress(const TDistributedChunkSessionProgress& progress)
    {
        auto guard = Guard(Lock_);
        Progress_ = progress;
    }

    void FailPings()
    {
        auto guard = Guard(Lock_);
        PingsFailed_ = true;
    }

    //! Keeps the session in the closing state until ReleaseFinishSession() is called.
    void HoldFinishSession()
    {
        auto guard = Guard(Lock_);
        FinishSessionHeld_ = true;
    }

    bool IsFinishSessionCaptured()
    {
        auto guard = Guard(Lock_);
        return static_cast<bool>(CapturedFinishSession_);
    }

    void ReleaseFinishSession()
    {
        TCtxFinishSessionPtr context;
        {
            auto guard = Guard(Lock_);
            FinishSessionHeld_ = false;
            context = std::move(CapturedFinishSession_);
        }

        if (context) {
            ReplyToFinishSession(context);
        }
    }

    i64 GetPingCount()
    {
        auto guard = Guard(Lock_);
        return PingCount_;
    }

private:
    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    TDistributedChunkSessionProgress Progress_;
    bool ProgressSuppressed_ = false;
    bool PingsFailed_ = false;
    bool FinishSessionHeld_ = false;
    i64 PingCount_ = 0;

    std::optional<TDistributedChunkSessionProgress> TryGetProgress()
    {
        auto guard = Guard(Lock_);
        if (ProgressSuppressed_) {
            return std::nullopt;
        }
        return Progress_;
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, StartSession)
    {
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PingSession)
    {
        bool pingsFailed;
        {
            auto guard = Guard(Lock_);
            ++PingCount_;
            pingsFailed = PingsFailed_;
        }

        if (pingsFailed) {
            context->Reply(TError(
                NChunkClient::EErrorCode::NoSuchSession,
                "Injected session loss"));
            return;
        }

        if (auto progress = TryGetProgress()) {
            ToProto(response->mutable_progress(), *progress);
        }
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, FinishSession)
    {
        {
            auto guard = Guard(Lock_);
            if (FinishSessionHeld_) {
                CapturedFinishSession_ = context;
                return;
            }
        }

        ReplyToFinishSession(context);
    }

    TCtxFinishSessionPtr CapturedFinishSession_;

    void ReplyToFinishSession(const TCtxFinishSessionPtr& context)
    {
        if (auto progress = TryGetProgress()) {
            ToProto(context->Response().mutable_progress(), *progress);
        }
        context->Reply();
    }
};

DEFINE_REFCOUNTED_TYPE(TFakeSequencerService)

using TFakeSequencerServicePtr = TIntrusivePtr<TFakeSequencerService>;

////////////////////////////////////////////////////////////////////////////////

//! Records every progress event in arrival order.
class TProgressCollector
    : public TRefCounted
{
public:
    void OnProgressUpdated(const TControllerSessionProgress& progress)
    {
        auto guard = Guard(Lock_);
        Events_.push_back(progress);
    }

    std::vector<TControllerSessionProgress> GetEvents()
    {
        auto guard = Guard(Lock_);
        return Events_;
    }

    i64 GetTerminalCount()
    {
        auto guard = Guard(Lock_);
        i64 count = 0;
        for (const auto& event : Events_) {
            if (!std::holds_alternative<TSessionInFlightProgress>(event)) {
                ++count;
            }
        }
        return count;
    }

private:
    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    std::vector<TControllerSessionProgress> Events_;
};

DEFINE_REFCOUNTED_TYPE(TProgressCollector)

using TProgressCollectorPtr = TIntrusivePtr<TProgressCollector>;

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionControllerTest
    : public ::testing::Test
{
protected:
    TActionQueuePtr ActionQueue_;
    IInvokerPtr Invoker_;
    TNodeDirectoryPtr NodeDirectory_;
    INodeMemoryTrackerPtr MemoryTracker_;
    TTestConnectionPtr Connection_;
    NApi::NNative::IClientPtr Client_;
    TFakeSequencerServicePtr Sequencer_;
    TFakeMasterServicePtr Master_;
    TChunkId ChunkId_;
    TTransactionId TransactionId_;

    void SetUp() override
    {
        ActionQueue_ = New<TActionQueue>("DcsController");
        Invoker_ = ActionQueue_->GetInvoker();
        NodeDirectory_ = New<TNodeDirectory>();
        MemoryTracker_ = CreateNodeMemoryTracker(32_MB, New<TNodeMemoryTrackerConfig>(), {});

        ChunkId_ = MakeRandomId(EObjectType::JournalChunk, TestCellTag);
        TransactionId_ = MakeRandomId(EObjectType::Transaction, TestCellTag);

        THashMap<std::string, IServicePtr> addressToService;
        Sequencer_ = New<TFakeSequencerService>(Invoker_);
        for (int index = 0; index < ReplicaCount; ++index) {
            auto address = std::string(Format("local:%v", index));
            NodeDirectory_->AddDescriptor(TNodeId(index), TNodeDescriptor(address));
            addressToService[address] = Sequencer_;
        }

        // Default fallback: TTestConnection generates synthetic master addresses.
        Master_ = New<TFakeMasterService>(Invoker_, ChunkId_, NodeDirectory_);
        auto channelFactory = CreateTestChannelFactory(addressToService, Master_);

        Connection_ = CreateConnection(
            std::move(channelFactory),
            {"default"},
            NodeDirectory_,
            /*nodeStatusDirectory*/ nullptr,
            Invoker_,
            MemoryTracker_);

        EXPECT_CALL(*Connection_, CreateNativeClient)
            .WillRepeatedly([this] (const NApi::NNative::TClientOptions& options) -> NApi::NNative::IClientPtr {
                return New<NApi::NNative::TClient>(Connection_, options, MemoryTracker_);
            });
        EXPECT_CALL(*Connection_, GetPrimaryMasterCellId).Times(testing::AnyNumber());
        EXPECT_CALL(*Connection_, GetPrimaryMasterCellTag).Times(testing::AnyNumber());
        EXPECT_CALL(*Connection_, GetSecondaryMasterCellTags).Times(testing::AnyNumber());
        EXPECT_CALL(*Connection_, GetClusterDirectory).Times(testing::AnyNumber());
        EXPECT_CALL(*Connection_, SubscribeReconfigured).Times(testing::AnyNumber());
        EXPECT_CALL(*Connection_, UnsubscribeReconfigured).Times(testing::AnyNumber());

        Client_ = Connection_->CreateNativeClient(
            NApi::NNative::TClientOptions::FromUser("test_user"));
    }

    void TearDown() override
    {
        Client_ = nullptr;
        Connection_ = nullptr;
        Sequencer_ = nullptr;
        Master_ = nullptr;
        if (MemoryTracker_) {
            MemoryTracker_->ClearTrackers();
            MemoryTracker_ = nullptr;
        }
        if (ActionQueue_) {
            ActionQueue_->Shutdown();
            ActionQueue_ = nullptr;
        }
    }

    IDistributedChunkSessionControllerPtr CreateController()
    {
        auto config = New<TDistributedChunkSessionControllerConfig>();
        config->SessionPingPeriod = TDuration::MilliSeconds(20);
        config->NodeRpcTimeout = TDuration::Minutes(2);
        config->CreateChunkTimeout = TDuration::Minutes(2);
        config->MaxConsecutivePingFailures = 2;

        auto writerOptions = New<TJournalChunkWriterOptions>();
        writerOptions->ReplicationFactor = ReplicaCount;
        writerOptions->ReadQuorum = 2;
        writerOptions->WriteQuorum = 2;

        return CreateDistributedChunkSessionController(
            Client_,
            std::move(config),
            TransactionId_,
            std::move(writerOptions),
            New<TJournalChunkWriterConfig>(),
            Invoker_);
    }

    static TDistributedChunkSessionProgress MakeProgress()
    {
        return TDistributedChunkSessionProgress{
            .DataWeight = 100,
            .CompressedDataSize = 200,
            .UncompressedDataSize = 300,
            .RecordCount = 4,
            .RowCount = 5,
        };
    }

    static void WaitUntil(const std::function<bool()>& predicate, TStringBuf message)
    {
        auto deadline = TInstant::Now() + TDuration::Seconds(30);
        while (!predicate()) {
            if (TInstant::Now() > deadline) {
                THROW_ERROR_EXCEPTION("Timed out: %v", message);
            }
            Sleep(TDuration::MilliSeconds(10));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDistributedChunkSessionControllerTest, FinalProgressIsReportedOnClose)
{
    Sequencer_->SetProgress(MakeProgress());

    auto controller = CreateController();
    WaitFor(controller->StartSession())
        .ThrowOnError();

    auto collector = New<TProgressCollector>();
    controller->SubscribeProgressUpdated(
        BIND(&TProgressCollector::OnProgressUpdated, collector));

    WaitFor(controller->Close())
        .ThrowOnError();

    EXPECT_EQ(collector->GetTerminalCount(), 1);

    auto events = collector->GetEvents();
    ASSERT_FALSE(events.empty());

    const auto* finalProgress = std::get_if<TSessionFinalProgress>(&events.back());
    ASSERT_NE(finalProgress, nullptr);
    ASSERT_TRUE(finalProgress->Underlying().has_value());
    EXPECT_EQ(*finalProgress->Underlying(), MakeProgress());
}

//! COMPAT(apollo1321): A pre-26.2 sequencer reports no progress at all, and the final
//! alternative must stay empty so that the pool can fall back to the master seal.
TEST_F(TDistributedChunkSessionControllerTest, LegacySequencerReportsEmptyFinalProgress)
{
    Sequencer_->SuppressProgress();

    auto controller = CreateController();
    WaitFor(controller->StartSession())
        .ThrowOnError();

    auto collector = New<TProgressCollector>();
    controller->SubscribeProgressUpdated(
        BIND(&TProgressCollector::OnProgressUpdated, collector));

    // Make sure at least one progressless ping has been handled.
    WaitUntil(
        [&] {
            return Sequencer_->GetPingCount() > 0;
        },
        "sequencer ping");

    WaitFor(controller->Close())
        .ThrowOnError();

    auto events = collector->GetEvents();
    ASSERT_FALSE(events.empty());

    const auto* finalProgress = std::get_if<TSessionFinalProgress>(&events.back());
    ASSERT_NE(finalProgress, nullptr);
    EXPECT_FALSE(finalProgress->Underlying().has_value());

    // The sequencer reported no progress at all, so nothing could have been published.
    EXPECT_EQ(std::ssize(events), 1);
}

//! The terminal alternative may be raised before the pool subscribes, so it has to be
//! replayed to a late subscriber.
TEST_F(TDistributedChunkSessionControllerTest, TerminalProgressIsReplayedToLateSubscriber)
{
    Sequencer_->SetProgress(MakeProgress());

    auto controller = CreateController();
    WaitFor(controller->StartSession())
        .ThrowOnError();

    WaitFor(controller->Close())
        .ThrowOnError();

    auto collector = New<TProgressCollector>();
    controller->SubscribeProgressUpdated(
        BIND(&TProgressCollector::OnProgressUpdated, collector));

    auto events = collector->GetEvents();
    ASSERT_EQ(std::ssize(events), 1);

    const auto* finalProgress = std::get_if<TSessionFinalProgress>(&events[0]);
    ASSERT_NE(finalProgress, nullptr);
    ASSERT_TRUE(finalProgress->Underlying().has_value());
    EXPECT_EQ(*finalProgress->Underlying(), MakeProgress());
}

//! In-flight progress precedes the single terminal alternative and never follows it.
TEST_F(TDistributedChunkSessionControllerTest, InFlightProgressPrecedesTerminalProgress)
{
    Sequencer_->SetProgress(MakeProgress());

    auto controller = CreateController();
    auto collector = New<TProgressCollector>();
    controller->SubscribeProgressUpdated(
        BIND(&TProgressCollector::OnProgressUpdated, collector));

    WaitFor(controller->StartSession())
        .ThrowOnError();

    WaitUntil(
        [&] {
            return !collector->GetEvents().empty();
        },
        "in-flight progress");

    WaitFor(controller->Close())
        .ThrowOnError();

    EXPECT_EQ(collector->GetTerminalCount(), 1);

    auto events = collector->GetEvents();
    ASSERT_GE(std::ssize(events), 2);
    for (int index = 0; index + 1 < std::ssize(events); ++index) {
        const auto* inFlightProgress = std::get_if<TSessionInFlightProgress>(&events[index]);
        ASSERT_NE(inFlightProgress, nullptr) << "Event " << index << " is terminal";
        EXPECT_EQ(inFlightProgress->Underlying(), MakeProgress());
    }
    EXPECT_TRUE(std::holds_alternative<TSessionFinalProgress>(events.back()));
}

//! A lost session terminates the controller with a close failure instead of a final
//! progress, even though nobody called Close().
TEST_F(TDistributedChunkSessionControllerTest, LostSessionRaisesCloseFailure)
{
    auto controller = CreateController();
    auto collector = New<TProgressCollector>();
    controller->SubscribeProgressUpdated(
        BIND(&TProgressCollector::OnProgressUpdated, collector));

    WaitFor(controller->StartSession())
        .ThrowOnError();

    Sequencer_->FailPings();

    auto closeError = WaitFor(controller->GetClosedFuture());
    EXPECT_TRUE(closeError.FindMatching(NChunkClient::EErrorCode::NoSuchSession))
        << Format("%v", closeError);

    EXPECT_EQ(collector->GetTerminalCount(), 1);

    auto events = collector->GetEvents();
    ASSERT_FALSE(events.empty());

    const auto* closeFailed = std::get_if<TSessionCloseFailed>(&events.back());
    ASSERT_NE(closeFailed, nullptr);
    EXPECT_TRUE(closeFailed->Underlying().FindMatching(NChunkClient::EErrorCode::NoSuchSession))
        << Format("%v", closeFailed->Underlying());
}

//! The session id stays observable while the session is closing, since a caller may look
//! it up between Close() and the closed future resolving.
TEST_F(TDistributedChunkSessionControllerTest, SessionIdIsAvailableWhileClosing)
{
    Sequencer_->SetProgress(MakeProgress());
    Sequencer_->HoldFinishSession();

    auto controller = CreateController();
    auto startedSession = WaitFor(controller->StartSession())
        .ValueOrThrow();

    auto closed = controller->Close();
    WaitUntil(
        [&] {
            return Sequencer_->IsFinishSessionCaptured();
        },
        "finish session request");

    EXPECT_EQ(controller->GetSessionId(), startedSession.SessionId);

    Sequencer_->ReleaseFinishSession();
    WaitFor(closed)
        .ThrowOnError();

    EXPECT_EQ(controller->GetSessionId(), startedSession.SessionId);
}

//! A session that never started still raises its terminal alternative, so a subscriber
//! installed before StartSession() is never left waiting for one.
TEST_F(TDistributedChunkSessionControllerTest, StartFailureRaisesCloseFailure)
{
    Master_->FailChunkCreation();

    auto controller = CreateController();
    auto collector = New<TProgressCollector>();
    controller->SubscribeProgressUpdated(
        BIND(&TProgressCollector::OnProgressUpdated, collector));

    EXPECT_FALSE(WaitFor(controller->StartSession()).IsOK());
    EXPECT_FALSE(WaitFor(controller->GetClosedFuture()).IsOK());

    auto events = collector->GetEvents();
    ASSERT_EQ(std::ssize(events), 1);
    EXPECT_TRUE(std::holds_alternative<TSessionCloseFailed>(events[0]));
}

//! Close() on an already terminated session neither hangs nor raises a second terminal.
TEST_F(TDistributedChunkSessionControllerTest, CloseAfterSessionLossRaisesNoSecondTerminal)
{
    auto controller = CreateController();
    auto collector = New<TProgressCollector>();
    controller->SubscribeProgressUpdated(
        BIND(&TProgressCollector::OnProgressUpdated, collector));

    WaitFor(controller->StartSession())
        .ThrowOnError();

    Sequencer_->FailPings();

    EXPECT_FALSE(WaitFor(controller->GetClosedFuture()).IsOK());
    EXPECT_FALSE(WaitFor(controller->Close()).IsOK());

    EXPECT_EQ(collector->GetTerminalCount(), 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDistributedChunkSessionClient
