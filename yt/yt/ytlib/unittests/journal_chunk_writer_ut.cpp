#include <yt/yt/ytlib/test_framework/test_connection.h>

#include <yt/yt/ytlib/journal_client/journal_chunk_writer.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_service.pb.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/journal_client.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/test_proxy_service.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NJournalClient {
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

const NLogging::TLogger TestLogger("JournalChunkWriterTest");

////////////////////////////////////////////////////////////////////////////////

//! A fake journal data node with TJournalSession::DoPutBlocks semantics:
//! gaps are rejected with MissingJournalChunkRecord, duplicates are skipped.
//! In manual mode requests are held and completed by the test in a chosen order.
class TFakeJournalNodeService
    : public TServiceBase
{
public:
    TFakeJournalNodeService(IInvokerPtr invoker, bool manualMode)
        : TServiceBase(
            std::move(invoker),
            TDataNodeServiceProxy::GetDescriptor(),
            TestLogger)
        , ManualMode_(manualMode)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks));
    }

    struct THeldRequestInfo
    {
        i64 FirstRecordIndex;
        i64 RecordCount;
    };

    struct TProcessedRequestInfo
    {
        i64 FirstRecordIndex;
        i64 RecordCount;
        bool Rejected;
    };

    i64 GetHeldRequestCount()
    {
        auto guard = Guard(Lock_);
        return std::ssize(HeldRequests_);
    }

    THeldRequestInfo GetHeldRequestInfo(int index)
    {
        auto guard = Guard(Lock_);
        YT_VERIFY(index < std::ssize(HeldRequests_));
        const auto& held = HeldRequests_[index];
        return {held.FirstRecordIndex, std::ssize(held.Records)};
    }

    void CompleteHeldRequest(int index)
    {
        THeldRequest held;
        {
            auto guard = Guard(Lock_);
            YT_VERIFY(index < std::ssize(HeldRequests_));
            held = std::move(HeldRequests_[index]);
            HeldRequests_.erase(HeldRequests_.begin() + index);
        }
        ProcessRequest(std::move(held));
    }

    void SwitchToAutoMode()
    {
        std::vector<THeldRequest> held;
        {
            auto guard = Guard(Lock_);
            ManualMode_ = false;
            held.swap(HeldRequests_);
        }
        for (auto& request : held) {
            ProcessRequest(std::move(request));
        }
    }

    i64 GetRecordCount()
    {
        auto guard = Guard(Lock_);
        return std::ssize(Records_);
    }

    std::vector<TSharedRef> GetRecords()
    {
        auto guard = Guard(Lock_);
        return Records_;
    }

    //! Replied PutBlocks requests, in reply order.
    std::vector<TProcessedRequestInfo> GetProcessedRequests()
    {
        auto guard = Guard(Lock_);
        return ProcessedRequests_;
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, StartChunk)
    {
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PingSession)
    {
        response->set_close_demanded(false);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PutBlocks)
    {
        YT_VERIFY(request->flush_blocks());

        THeldRequest held{
            .Context = context,
            .FirstRecordIndex = request->first_block_index(),
            .Records = std::vector<TSharedRef>(
                request->Attachments().begin(),
                request->Attachments().end()),
        };

        {
            auto guard = Guard(Lock_);
            if (ManualMode_) {
                HeldRequests_.push_back(std::move(held));
                return;
            }
        }

        ProcessRequest(std::move(held));
    }

    struct THeldRequest
    {
        TCtxPutBlocksPtr Context;
        i64 FirstRecordIndex = 0;
        std::vector<TSharedRef> Records;
    };

    TSpinLock Lock_;
    bool ManualMode_;
    std::vector<THeldRequest> HeldRequests_;
    std::vector<TSharedRef> Records_;
    std::vector<TProcessedRequestInfo> ProcessedRequests_;

    void ProcessRequest(THeldRequest held)
    {
        TError error;
        {
            auto guard = Guard(Lock_);
            i64 recordCount = std::ssize(Records_);
            if (held.FirstRecordIndex > recordCount) {
                error = TError(NChunkClient::EErrorCode::MissingJournalChunkRecord, "Missing blocks")
                    << TErrorAttribute("start_block_index", recordCount)
                    << TErrorAttribute("end_block_index", held.FirstRecordIndex - 1);
            } else {
                for (i64 index = recordCount - held.FirstRecordIndex; index < std::ssize(held.Records); ++index) {
                    Records_.push_back(held.Records[index]);
                }
            }
            ProcessedRequests_.push_back({
                .FirstRecordIndex = held.FirstRecordIndex,
                .RecordCount = std::ssize(held.Records),
                .Rejected = !error.IsOK(),
            });
        }

        if (error.IsOK()) {
            held.Context->Reply();
        } else {
            held.Context->Reply(error);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TFakeJournalNodeService)

using TFakeJournalNodeServicePtr = TIntrusivePtr<TFakeJournalNodeService>;

////////////////////////////////////////////////////////////////////////////////

//! Serves ConfirmChunk, which TJournalChunkWriter invokes on open.
class TFakeMasterService
    : public TServiceBase
{
public:
    explicit TFakeMasterService(IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            TChunkServiceProxy::GetDescriptor(),
            TestLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ConfirmChunk));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ConfirmChunk)
    {
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TJournalChunkWriterTest
    : public ::testing::Test
{
protected:
    TActionQueuePtr ActionQueue_;
    IInvokerPtr Invoker_;
    TNodeDirectoryPtr NodeDirectory_;
    INodeMemoryTrackerPtr MemoryTracker_;
    TTestConnectionPtr Connection_;
    std::vector<TFakeJournalNodeServicePtr> NodeServices_;
    TChunkId ChunkId_;
    IJournalChunkWriterPtr Writer_;

    void Initialize(
        const TJournalChunkWriterConfigPtr& config,
        const THashSet<int>& manualNodes)
    {
        ActionQueue_ = New<TActionQueue>("JournalWriter");
        Invoker_ = ActionQueue_->GetInvoker();
        NodeDirectory_ = New<TNodeDirectory>();
        MemoryTracker_ = CreateNodeMemoryTracker(32_MB, New<TNodeMemoryTrackerConfig>(), {});

        ChunkId_ = MakeRandomId(EObjectType::JournalChunk, TCellTag(0xf003));

        THashMap<std::string, IServicePtr> addressToService;
        TChunkReplicaWithMediumList replicas;
        for (int index = 0; index < ReplicaCount; ++index) {
            auto address = std::string(Format("local:%v", index));
            NodeDirectory_->AddDescriptor(TNodeId(index), TNodeDescriptor(address));
            NodeServices_.push_back(New<TFakeJournalNodeService>(
                Invoker_,
                /*manualMode*/ manualNodes.contains(index)));
            addressToService[address] = NodeServices_.back();
            replicas.push_back(TChunkReplicaWithMedium(
                TNodeId(index),
                GenericChunkReplicaIndex,
                AllMediaIndex));
        }

        auto masterService = New<TFakeMasterService>(Invoker_);
        auto masterAddress = Format("master-%v-tag-%v",
            EMasterChannelKind::Leader,
            CellTagFromId(ChunkId_));
        addressToService[masterAddress] = masterService;

        // Default fallback: TTestConnection generates synthetic master addresses.
        auto channelFactory = CreateTestChannelFactory(
            addressToService,
            masterService);

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

        auto client = Connection_->CreateNativeClient(
            NApi::NNative::TClientOptions::FromUser("test_user"));

        auto options = New<TJournalChunkWriterOptions>();
        options->ReplicationFactor = ReplicaCount;
        options->ReadQuorum = 2;
        options->WriteQuorum = 2;
        options->ReplicaLagLimit = 1'000'000;
        options->EnableMultiplexing = false;

        Writer_ = CreateJournalChunkWriter(
            std::move(client),
            TSessionId(ChunkId_, GenericMediumIndex),
            std::move(options),
            config,
            TJournalWriterPerformanceCounters{},
            Invoker_,
            replicas,
            EChunkFormat::JournalDefault,
            TestLogger);
    }

    void TearDown() override
    {
        Writer_ = nullptr;
        Connection_ = nullptr;
        NodeServices_.clear();
        if (MemoryTracker_) {
            MemoryTracker_->ClearTrackers();
            MemoryTracker_ = nullptr;
        }
        if (ActionQueue_) {
            ActionQueue_->Shutdown();
            ActionQueue_ = nullptr;
        }
    }

    static TJournalChunkWriterConfigPtr CreateWriterConfig()
    {
        auto config = New<TJournalChunkWriterConfig>();
        config->NodeRpcTimeout = TDuration::Minutes(2);
        return config;
    }

    static TSharedRef MakeRecord(i64 index)
    {
        return TSharedRef::FromString(Format("record-%v", index));
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

    static void ExpectRecords(const TFakeJournalNodeServicePtr& service, i64 recordCount)
    {
        auto records = service->GetRecords();
        ASSERT_EQ(std::ssize(records), recordCount);
        for (i64 index = 0; index < recordCount; ++index) {
            auto expected = MakeRecord(index);
            EXPECT_EQ(
                TStringBuf(records[index].Begin(), records[index].Size()),
                TStringBuf(expected.Begin(), expected.Size()))
                << "Record " << index << " content mismatch";
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Replica 0 never replies, yet the writer must keep MaxInFlightFlushCount
//! unreplied flushes in flight to it.
TEST_F(TJournalChunkWriterTest, PipelinedWrite)
{
    auto config = CreateWriterConfig();
    config->MaxInFlightFlushCount = 4;
    config->MaxBatchDelay = TDuration::Zero();
    config->MaxFlushRowCount = 1;
    Initialize(config, /*manualNodes*/ {0});

    WaitFor(Writer_->Open())
        .ThrowOnError();

    const auto& manualNode = NodeServices_[0];

    constexpr int RecordCount = 6;
    std::vector<TFuture<void>> recordFutures;
    recordFutures.reserve(RecordCount);
    for (i64 index = 0; index < RecordCount; ++index) {
        recordFutures.push_back(Writer_->WriteRecord(MakeRecord(index)).AsVoid());
    }

    WaitUntil(
        [&] { return manualNode->GetHeldRequestCount() == 4; },
        "Waiting for the flush pipeline to fill up");
    for (int index = 0; index < 4; ++index) {
        ASSERT_EQ(manualNode->GetHeldRequestInfo(index).FirstRecordIndex, index);
        ASSERT_EQ(manualNode->GetHeldRequestInfo(index).RecordCount, 1);
    }

    // Records are acked by the quorum of replicas 1 and 2 alone.
    WaitFor(AllSucceeded(std::move(recordFutures)))
        .ThrowOnError();
    ASSERT_EQ(manualNode->GetHeldRequestCount(), 4);

    // Completing one request frees exactly one pipeline slot.
    manualNode->CompleteHeldRequest(0);
    WaitUntil(
        [&] { return manualNode->GetHeldRequestCount() == 4; },
        "Waiting for the freed pipeline slot to be refilled");
    ASSERT_EQ(manualNode->GetHeldRequestInfo(3).FirstRecordIndex, 4);

    manualNode->SwitchToAutoMode();

    WaitFor(Writer_->Close())
        .ThrowOnError();

    for (const auto& service : NodeServices_) {
        ExpectRecords(service, RecordCount);
    }
}

//! Replica 0 holds flushes A=[0,9] and B=[10,19]; B is rejected as if it
//! overtook A at the node, then A succeeds. The writer must resend exactly
//! the records replica 0 is missing and close cleanly.
TEST_F(TJournalChunkWriterTest, OutOfOrderFlushRejectionRecovery)
{
    auto config = CreateWriterConfig();
    config->MaxInFlightFlushCount = 2;
    config->MaxBatchDelay = TDuration::Seconds(3);
    config->MaxBatchRowCount = 1'000;
    Initialize(config, /*manualNodes*/ {0});

    WaitFor(Writer_->Open())
        .ThrowOnError();

    const auto& manualNode = NodeServices_[0];

    std::vector<TFuture<void>> recordFutures;
    recordFutures.reserve(20);

    // First batch: A=[0,9].
    for (i64 index = 0; index < 10; ++index) {
        recordFutures.push_back(Writer_->WriteRecord(MakeRecord(index)).AsVoid());
    }
    WaitUntil(
        [&] { return manualNode->GetHeldRequestCount() == 1; },
        "Waiting for the first flush to replica 0");
    ASSERT_EQ(manualNode->GetHeldRequestInfo(0).FirstRecordIndex, 0);
    ASSERT_EQ(manualNode->GetHeldRequestInfo(0).RecordCount, 10);

    // Second batch: B=[10,19].
    for (i64 index = 10; index < 20; ++index) {
        recordFutures.push_back(Writer_->WriteRecord(MakeRecord(index)).AsVoid());
    }
    WaitUntil(
        [&] { return manualNode->GetHeldRequestCount() == 2; },
        "Waiting for the second flush to replica 0");
    ASSERT_EQ(manualNode->GetHeldRequestInfo(1).FirstRecordIndex, 10);
    ASSERT_EQ(manualNode->GetHeldRequestInfo(1).RecordCount, 10);

    // Replicas 1 and 2 have confirmed everything, forming a write quorum.
    WaitFor(AllSucceeded(std::move(recordFutures)))
        .ThrowOnError();

    // B is rejected: it arrived at the node before A.
    manualNode->CompleteHeldRequest(1);
    Sleep(TDuration::MilliSeconds(500));

    // A succeeds; records 0..9 become fully replicated and are evicted.
    manualNode->CompleteHeldRequest(0);
    Sleep(TDuration::MilliSeconds(500));

    manualNode->SwitchToAutoMode();

    WaitFor(Writer_->Close())
        .ThrowOnError();

    ExpectRecords(manualNode, 20);

    // B rejected, A confirmed, then a single resend of exactly [10, 19].
    auto processedRequests = manualNode->GetProcessedRequests();
    ASSERT_EQ(std::ssize(processedRequests), 3);
    EXPECT_EQ(processedRequests[0].FirstRecordIndex, 10);
    EXPECT_EQ(processedRequests[0].RecordCount, 10);
    EXPECT_TRUE(processedRequests[0].Rejected);
    EXPECT_EQ(processedRequests[1].FirstRecordIndex, 0);
    EXPECT_EQ(processedRequests[1].RecordCount, 10);
    EXPECT_FALSE(processedRequests[1].Rejected);
    EXPECT_EQ(processedRequests[2].FirstRecordIndex, 10);
    EXPECT_EQ(processedRequests[2].RecordCount, 10);
    EXPECT_FALSE(processedRequests[2].Rejected);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NJournalClient
