#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_controller.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_writer.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_allowing_repair.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/journal_client/chunk_reader.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/suspendable_action_queue.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

#include <library/cpp/yt/misc/range_helpers.h>

namespace NYT {
namespace NCppTests {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NErasure;
using namespace NJournalClient;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;

using testing::HasSubstr;
using testing::UnorderedElementsAreArray;
using testing::Combine;
using testing::ValuesIn;

using NChunkClient::NProto::TMiscExt;

using NJournalClient::TChunkReaderConfig;
using NJournalClient::CreateChunkReader;

////////////////////////////////////////////////////////////////////////////////

std::string MakeRandomString(size_t stringSize)
{
    std::string randomString;
    randomString.reserve(stringSize);
    for (size_t i = 0; i < stringSize; i++) {
        randomString += ('a' + rand() % 25);
    }
    return randomString;
}

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionTest
    : public TApiTestBase
{
protected:
    NNative::IClientPtr NativeClient_;
    NNative::IConnectionPtr NativeConnection_;
    ISuspendableActionQueuePtr ActionQueue_;
    ITransactionPtr Transaction_;
    TDistributedChunkSessionControllerConfigPtr ControllerConfig_;
    TJournalChunkWriterConfigPtr WriterConfig_;
    TJournalChunkWriterOptionsPtr WriterOptions_;

    static constexpr bool EnableDebugOutput = false;

    void SetUp() override
    {
        if (EnableDebugOutput) {
            auto config = TLogManagerConfig::CreateStderrLogger(ELogLevel::Debug);
            config->AbortOnAlert = true;
            TLogManager::Get()->Configure(config, /*sync*/ true);
        }

        NativeClient_ = DynamicPointerCast<NNative::IClient>(Client_);
        NativeConnection_ = NativeClient_->GetNativeConnection();

        ActionQueue_ = CreateSuspendableActionQueue("TestSession");

        Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();

        ControllerConfig_ = New<TDistributedChunkSessionControllerConfig>();
        ControllerConfig_->Account = "intermediate";
        ControllerConfig_->SessionPingPeriod = TDuration::MilliSeconds(100);
        ControllerConfig_->SessionTimeout = TDuration::Seconds(1);

        WriterConfig_ = New<TJournalChunkWriterConfig>();
        WriterOptions_ = New<TJournalChunkWriterOptions>();

        WriterOptions_->ReplicationFactor = 3;
    }

    IChunkReaderPtr CreateQuorumReader(TChunkId chunkId)
    {
        auto replicaStrings = ConvertTo<std::vector<std::string>>(
            WaitFor(NativeClient_->GetNode(Format("#%v/@stored_replicas", chunkId)))
                .ValueOrThrow());

        YT_VERIFY(!replicaStrings.empty());

        auto channelFactory = NativeConnection_->GetChannelFactory();

        std::string bestAddress;
        std::optional<i64> bestRowCount;

        for (const auto& address : replicaStrings) {
            auto channel = channelFactory->CreateChannel(address);
            TDataNodeServiceProxy proxy(channel);

            auto req = proxy.GetChunkMeta();
            ToProto(req->mutable_chunk_id(), chunkId);
            req->set_all_extension_tags(true);

            auto rspOrError = WaitFor(req->Invoke());
            if (!rspOrError.IsOK()) {
                continue;
            }

            const auto& meta = rspOrError.Value()->chunk_meta();
            auto miscExt = GetProtoExtension<TMiscExt>(
                meta.extensions());

            if (!bestRowCount || miscExt.row_count() > *bestRowCount) {
                bestRowCount = miscExt.row_count();
                bestAddress = address;
            }
        }

        YT_VERIFY(!bestAddress.empty());

        auto nodeDirectory = NativeConnection_->GetNodeDirectory();

        TNodeId bestNodeId;
        for (const auto& [nodeId, descriptor] : nodeDirectory->GetAllDescriptors()) {
            if (descriptor.GetDefaultAddress() == bestAddress) {
                bestNodeId = nodeId;
                break;
            }
        }
        YT_VERIFY(bestNodeId != TNodeId{});

        TChunkReplicaWithMedium replica(
            bestNodeId,
            /*replicaIndex*/ 0,
            /*mediumIndex*/ GenericMediumIndex);

        return CreateChunkReader(
            New<NJournalClient::TChunkReaderConfig>(),
            New<TRemoteReaderOptions>(),
            New<TChunkReaderHost>(NativeClient_),
            chunkId,
            ECodec::None,
            /*replicas*/ {replica});
    }

    static void EnsureControllerIsDestroyed(IDistributedChunkSessionControllerPtr controller)
    {
        auto controllerWeakPtr = TWeakPtr(controller);
        controller.Reset();
        EXPECT_TRUE(controllerWeakPtr.IsExpired());
    }

    static std::vector<std::string> GetChunkReplicaNodes(TChunkId chunkId)
    {
        return ConvertTo<std::vector<std::string>>(
            WaitFor(Client_->GetNode(Format("#%v/@stored_replicas", chunkId)))
                .ValueOrThrow());
    }

    static void UnbanDataNode(const std::string& dataNode)
    {
        WaitFor(Client_->RemoveMaintenance(EMaintenanceComponent::ClusterNode, dataNode, {}))
            .ThrowOnError();
        WaitUntil(
            [&] {
                return ConvertTo<ENodeState>(
                    WaitFor(Client_->GetNode(Format("//sys/cluster_nodes/%v/@state", dataNode)))
                        .ValueOrThrow()) == ENodeState::Online;
            },
            "Node is offline");
    }

    static std::vector<std::string> GetDataNodes()
    {
        return ConvertTo<std::vector<std::string>>(
            WaitFor(Client_->ListNode(Format("//sys/data_nodes")))
                .ValueOrThrow());
    }

    [[nodiscard]] static auto BanDataNode(const std::string& dataNode)
    {
        WaitFor(Client_->AddMaintenance(EMaintenanceComponent::ClusterNode, dataNode, EMaintenanceType::Ban, "test"))
            .ThrowOnError();
        WaitUntil(
            [&] {
                return ConvertTo<ENodeState>(
                    WaitFor(Client_->GetNode(Format("//sys/cluster_nodes/%v/@state", dataNode)))
                        .ValueOrThrow()) == ENodeState::Offline;
            },
            "Node is online");
        return Finally([dataNode] {
            UnbanDataNode(dataNode);
        });
    }

    [[nodiscard]] static auto BanDataNodes(const std::vector<std::string>& dataNodes)
    {
        return TransformRangeTo<std::vector<decltype(BanDataNode(std::declval<std::string>()))>>(
            dataNodes,
            &BanDataNode);
    }
};

////////////////////////////////////////////////////////////////////////////////

static int GetPidByPort(int port)
{
    // TODO(apollo1321): This implementation is a hacky workaround.
    // Ideally, the test environment (Python recipe) should expose node pids
    // directly, e.g. via a file or environment variable, so that tests can
    // interact with node processes without resorting to parsing ss output.
    // This should be reworked once proper test environment integration is
    // available.

    // Use ss to find pid listening on the given port.
    // -t: TCP, -l: listening, -n: numeric, -p: show process
    auto command = Format("ss -tlnp 'sport = :%v'", port);
    FILE* pipe = popen(command.c_str(), "r");
    YT_VERIFY(pipe);

    char buffer[256];
    std::string output;
    while (fgets(buffer, sizeof(buffer), pipe)) {
        output += buffer;
    }
    pclose(pipe);

    // Output looks like:
    // State  Recv-Q  Send-Q  Local Address:Port  Peer Address:Port  Process
    // LISTEN 0       128     *:9012             *:*                users:(("ytserver-node",pid=12345,fd=10))
    auto pidPos = output.find("pid=");
    YT_VERIFY(pidPos != std::string::npos);

    auto pidStart = pidPos + 4;
    auto pidEnd = output.find(',', pidStart);
    YT_VERIFY(pidEnd != std::string::npos);

    return std::stoi(output.substr(pidStart, pidEnd - pidStart));
}

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] static auto PauseProcess(const std::string& dataNode)
{
    auto colonPos = dataNode.rfind(':');
    int port = std::stoi(dataNode.substr(colonPos + 1));
    int pid = GetPidByPort(port);

    YT_VERIFY(kill(pid, SIGSTOP) == 0);

    return Finally([pid] {
        YT_VERIFY(kill(pid, SIGCONT) == 0);
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDistributedChunkSessionTest, SingleWriter)
{
    WriterOptions_->WriteQuorum = 2;
    WriterOptions_->ReplicationFactor = 3;

    auto controller = CreateDistributedChunkSessionController(
        NativeClient_,
        ControllerConfig_,
        Transaction_->GetId(),
        WriterOptions_,
        WriterConfig_,
        ActionQueue_->GetInvoker());

    auto sequencerNode = WaitFor(controller->StartSession())
        .ValueOrThrow();

    // Ensure that controller sends pings to sequencer and session stays alive.
    Sleep(TDuration::Seconds(2));

    auto writer = CreateDistributedChunkWriter(
        sequencerNode,
        controller->GetSessionId(),
        NativeConnection_,
        New<TDistributedChunkWriterConfig>());

    auto record = MakeRandomString(100);

    WaitFor(writer->WriteRecord(TSharedRef::FromString(record)))
        .ThrowOnError();

    auto reader = CreateQuorumReader(controller->GetSessionId().ChunkId);

    auto blocks = WaitFor(reader->ReadBlocks(IChunkReader::TReadBlocksOptions{}, {0}))
        .ValueOrThrow();

    ASSERT_EQ(std::ssize(blocks), 1);
    EXPECT_EQ(blocks[0].Data.ToStringBuf(), record);

    EnsureControllerIsDestroyed(std::move(controller));
}

TEST_F(TDistributedChunkSessionTest, MultipleWriters)
{
    WriterOptions_->WriteQuorum = 2;
    WriterOptions_->ReplicationFactor = 3;

    auto controller = CreateDistributedChunkSessionController(
        NativeClient_,
        ControllerConfig_,
        Transaction_->GetId(),
        WriterOptions_,
        WriterConfig_,
        ActionQueue_->GetInvoker());

    auto sequencerNode = WaitFor(controller->StartSession())
        .ValueOrThrow();

    Sleep(TDuration::Seconds(2));

    constexpr int WriterCount = 3;
    constexpr int RecordsPerWriter = 5;
    const int totalRecords = WriterCount * RecordsPerWriter;

    // Each writer sends all its records concurrently and we collect
    // the futures to wait on all of them at once.
    std::vector<std::string> expectedRecords(totalRecords);
    std::vector<TFuture<void>> writeFutures(totalRecords);

    for (int writerIdx = 0; writerIdx < WriterCount; ++writerIdx) {
        auto writer = CreateDistributedChunkWriter(
            sequencerNode,
            controller->GetSessionId(),
            NativeConnection_,
            New<TDistributedChunkWriterConfig>());

        for (int recordIdx = 0; recordIdx < RecordsPerWriter; ++recordIdx) {
            int idx = writerIdx * RecordsPerWriter + recordIdx;
            expectedRecords[idx] = MakeRandomString(100);
            writeFutures[idx] = writer->WriteRecord(TSharedRef::FromString(expectedRecords[idx]));
        }
    }

    WaitFor(AllSucceeded(writeFutures))
        .ThrowOnError();

    auto reader = CreateQuorumReader(controller->GetSessionId().ChunkId);

    auto blocks = WaitFor(reader->ReadBlocks(IChunkReader::TReadBlocksOptions{}, 0, totalRecords))
        .ValueOrThrow();

    ASSERT_EQ(std::ssize(blocks), totalRecords);

    // Records may arrive in any order since writers are concurrent —
    // verify the set of records matches rather than exact ordering.
    std::vector<std::string> actualRecords;
    actualRecords.reserve(totalRecords);
    for (const auto& block : blocks) {
        actualRecords.push_back(std::string(block.Data.ToStringBuf()));
    }

    EXPECT_THAT(actualRecords, UnorderedElementsAreArray(expectedRecords));

    EnsureControllerIsDestroyed(std::move(controller));
}

TEST_F(TDistributedChunkSessionTest, SessionTimeout)
{
    auto controller = CreateDistributedChunkSessionController(
        NativeClient_,
        ControllerConfig_,
        Transaction_->GetId(),
        WriterOptions_,
        WriterConfig_,
        ActionQueue_->GetInvoker());

    auto sequencerNode = WaitFor(controller->StartSession())
        .ValueOrThrow();

    WaitFor(ActionQueue_->Suspend(true))
        .ThrowOnError();

    Sleep(TDuration::Seconds(2));

    auto writer = CreateDistributedChunkWriter(
        sequencerNode,
        controller->GetSessionId(),
        NativeConnection_,
        New<TDistributedChunkWriterConfig>());

    auto writeError = WaitFor(writer->WriteRecord(TSharedRef::FromString(MakeRandomString(100))));
    EXPECT_FALSE(writeError.IsOK());
    EXPECT_EQ(writeError.GetCode(), NChunkClient::EErrorCode::NoSuchSession);
    EXPECT_THAT(writeError.GetMessage(), HasSubstr("invalid or expired"));

    ActionQueue_->Resume();

    // After resuming, the controller detects the expired session via ping and closes.
    auto closedError = WaitFor(controller->WaitUntilClosed());
    EXPECT_FALSE(closedError.IsOK());
    EXPECT_EQ(closedError.GetCode(), NChunkClient::EErrorCode::NoSuchSession);

    EnsureControllerIsDestroyed(std::move(controller));
}

TEST_F(TDistributedChunkSessionTest, NotEnoughNodesToWrite)
{
    WriterOptions_->WriteQuorum = 2;
    WriterOptions_->ReplicationFactor = 3;

    auto controller = CreateDistributedChunkSessionController(
        NativeClient_,
        ControllerConfig_,
        Transaction_->GetId(),
        WriterOptions_,
        WriterConfig_,
        ActionQueue_->GetInvoker());

    auto sequencerNode = WaitFor(controller->StartSession())
        .ValueOrThrow();

    auto nodes = GetDataNodes();

    auto it = std::find(nodes.begin(), nodes.end(), sequencerNode.GetDefaultAddress());
    ASSERT_TRUE(it != nodes.end());
    nodes.erase(it);
    ASSERT_EQ(std::ssize(nodes), 2);

    auto banGuard = BanDataNodes(nodes);

    auto writer = CreateDistributedChunkWriter(
        sequencerNode,
        controller->GetSessionId(),
        NativeConnection_,
        New<TDistributedChunkWriterConfig>());

    auto writeError = WaitFor(writer->WriteRecord(TSharedRef::FromString(MakeRandomString(100))));
    EXPECT_FALSE(writeError.IsOK());
    EXPECT_THAT(writeError.GetMessage(), HasSubstr("Journal chunk writer failed"));
}

TEST_F(TDistributedChunkSessionTest, DoubleClose)
{
    WriterOptions_->WriteQuorum = 2;
    WriterOptions_->ReplicationFactor = 3;

    auto controller = CreateDistributedChunkSessionController(
        NativeClient_,
        ControllerConfig_,
        Transaction_->GetId(),
        WriterOptions_,
        WriterConfig_,
        ActionQueue_->GetInvoker());

    WaitFor(controller->StartSession())
        .ThrowOnError();

    auto firstClose  = controller->Close();
    auto secondClose = controller->Close();

    WaitFor(firstClose)
        .ThrowOnError();
    WaitFor(secondClose)
        .ThrowOnError();

    EnsureControllerIsDestroyed(std::move(controller));
}

TEST_F(TDistributedChunkSessionTest, StartSessionWithNotEnoughNodes)
{
    WriterOptions_->WriteQuorum = 2;
    WriterOptions_->ReplicationFactor = 3;

    auto nodes = GetDataNodes();
    ASSERT_GE(std::ssize(nodes), 2);

    // Ban all nodes except one so AllocateWriteTargets cannot
    // satisfy minTargetCount == ReplicationFactor.
    auto banGuard = BanDataNodes(
        std::vector<std::string>(nodes.begin() + 1, nodes.end()));

    auto controller = CreateDistributedChunkSessionController(
        NativeClient_,
        ControllerConfig_,
        Transaction_->GetId(),
        WriterOptions_,
        WriterConfig_,
        ActionQueue_->GetInvoker());

    auto startError = WaitFor(controller->StartSession());
    EXPECT_FALSE(startError.IsOK());
    EXPECT_THAT(startError.GetMessage(), HasSubstr("Not enough"));

    // WaitUntilClosed must already be resolved with the same error
    // since DoStartSession set ClosedPromise_ on failure.
    auto closedError = WaitFor(controller->WaitUntilClosed());
    EXPECT_FALSE(closedError.IsOK());
    EXPECT_EQ(closedError.GetCode(), startError.GetCode());

    // Close() on a failed-to-start controller must not hang or crash.
    auto closeError = WaitFor(controller->Close());
    EXPECT_FALSE(closeError.IsOK());
    EXPECT_EQ(closeError.GetCode(), startError.GetCode());

    EnsureControllerIsDestroyed(std::move(controller));
}

TEST_F(TDistributedChunkSessionTest, WaitUntilClosedResolvedOnSessionExpiry)
{
    ControllerConfig_->SessionTimeout = TDuration::Seconds(1);
    ControllerConfig_->SessionPingPeriod = TDuration::MilliSeconds(200);

    WriterOptions_->WriteQuorum = 2;
    WriterOptions_->ReplicationFactor = 3;

    auto controller = CreateDistributedChunkSessionController(
        NativeClient_,
        ControllerConfig_,
        Transaction_->GetId(),
        WriterOptions_,
        WriterConfig_,
        ActionQueue_->GetInvoker());

    WaitFor(controller->StartSession())
        .ThrowOnError();

    // Suspend pings so the session expires on the server side.
    WaitFor(ActionQueue_->Suspend(/*immediately*/ true))
        .ThrowOnError();

    // Wait for server-side lease to expire.
    Sleep(TDuration::Seconds(2));

    // Resume so the ping executor can fire and detect the expired session,
    // which sets ClosedPromise_ with an error.
    ActionQueue_->Resume();

    auto error = WaitFor(controller->WaitUntilClosed());
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(error.GetCode(), NChunkClient::EErrorCode::NoSuchSession);

    EnsureControllerIsDestroyed(std::move(controller));
}

TEST_F(TDistributedChunkSessionTest, SequencerNodeDiesAfterWrite)
{
    ControllerConfig_->SessionPingPeriod = TDuration::MilliSeconds(200);
    ControllerConfig_->MaxConsecutivePingFailures = 3;

    WriterOptions_->WriteQuorum = 2;
    WriterOptions_->ReplicationFactor = 3;

    auto controller = CreateDistributedChunkSessionController(
        NativeClient_,
        ControllerConfig_,
        Transaction_->GetId(),
        WriterOptions_,
        WriterConfig_,
        ActionQueue_->GetInvoker());

    auto sequencerNode = WaitFor(controller->StartSession())
        .ValueOrThrow();

    auto writer = CreateDistributedChunkWriter(
        sequencerNode,
        controller->GetSessionId(),
        NativeConnection_,
        New<TDistributedChunkWriterConfig>());

    auto record = MakeRandomString(100);

    WaitFor(writer->WriteRecord(TSharedRef::FromString(record)))
        .ThrowOnError();

    auto chunkId = controller->GetSessionId().ChunkId;

    {
        // Pause the sequencer process via SIGSTOP — pings will time out
        // since the process is frozen. After MaxConsecutivePingFailures
        // the controller closes with an error.
        auto resumeGuard = PauseProcess(sequencerNode.GetDefaultAddress());

        auto error = WaitFor(controller->WaitUntilClosed());
        EXPECT_FALSE(error.IsOK());
        EXPECT_THAT(error.GetMessage(), HasSubstr("Too many consecutive ping failures"));
    }

    // The record was written with WriteQuorum=2 so it must be
    // available on the surviving replicas despite the sequencer being paused.
    auto reader = CreateQuorumReader(chunkId);

    auto blocks = WaitFor(reader->ReadBlocks(IChunkReader::TReadBlocksOptions{}, {0}))
        .ValueOrThrow();

    ASSERT_EQ(std::ssize(blocks), 1);
    EXPECT_EQ(blocks[0].Data.ToStringBuf(), record);

    EnsureControllerIsDestroyed(std::move(controller));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCppTests
} // namespace NYT
