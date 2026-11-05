#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/server/lib/distributed_chunk_session_server/session_service.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/service_proxy.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/session_reader.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/statistics.h>

#include <yt/yt/ytlib/journal_client/chunk_reader.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_builder.h>

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/test_framework/test_proxy_service.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionNodeDirectoryTest
    : public TApiTestBase
    , public ::testing::WithParamInterface<bool>
{ };

TEST_P(TDistributedChunkSessionNodeDirectoryTest, StartSession)
{
    const auto populateKnownTargets = GetParam();
    auto client = DynamicPointerCast<NNative::IClient>(Client_);
    auto transaction = WaitFor(client->StartTransaction(NTransactionClient::ETransactionType::Master))
        .ValueOrThrow();

    auto writerOptions = New<TJournalChunkWriterOptions>();
    writerOptions->ReplicationFactor = 3;
    writerOptions->WriteQuorum = 2;

    TChunkServiceProxy masterProxy(client->GetMasterChannelOrThrow(
        EMasterChannelKind::Leader,
        CellTagFromId(transaction->GetId())));
    auto createReq = masterProxy.CreateChunk();
    NRpc::GenerateMutationId(createReq);
    createReq->set_type(ToProto(EObjectType::JournalChunk));
    createReq->set_account("intermediate");
    ToProto(createReq->mutable_transaction_id(), transaction->GetId());
    createReq->set_replication_factor(writerOptions->ReplicationFactor);
    createReq->set_erasure_codec(ToProto(writerOptions->ErasureCodec));
    createReq->set_medium_name("default");
    createReq->set_read_quorum(writerOptions->ReadQuorum);
    createReq->set_write_quorum(writerOptions->WriteQuorum);

    auto createRsp = WaitFor(createReq->Invoke())
        .ValueOrThrow();
    auto sessionId = FromProto<TSessionId>(createRsp->session_id());

    auto targets = AllocateWriteTargets(
        client,
        sessionId,
        /*desiredTargetCount*/ writerOptions->ReplicationFactor,
        /*minTargetCount*/ writerOptions->ReplicationFactor);

    auto connection = NNative::CreateConnection(client->GetNativeConnection()->GetCompoundConfig());
    auto terminateConnection = Finally([&] { connection->Terminate(); });
    ASSERT_TRUE(connection->GetNodeDirectory()->GetAllDescriptors().empty());
    const auto unrelatedNodeId = TNodeId(100'000);
    connection->GetNodeDirectory()->AddDescriptor(unrelatedNodeId, TNodeDescriptor(std::string("unrelated:1")));

    auto actionQueue = New<TActionQueue>("TestSequencer");
    auto service = NDistributedChunkSessionServer::CreateDistributedChunkSessionService(
        actionQueue->GetInvoker(),
        connection);
    auto channelFactory = NRpc::CreateTestChannelFactoryWithDefaultServices(service);
    TDistributedChunkSessionServiceProxy proxy(channelFactory->CreateChannel("local:sequencer"));

    auto makeStartRequest = [&] {
        auto req = proxy.StartSession();
        ToProto(req->mutable_session_id(), sessionId);
        req->set_session_timeout(ToProto(TDuration::Minutes(1)));
        ToProto(req->mutable_chunk_replicas(), targets);
        req->set_journal_chunk_writer_options(ToProto(ConvertToYsonString(writerOptions)));
        req->set_journal_chunk_writer_config(ToProto(ConvertToYsonString(New<TJournalChunkWriterConfig>())));
        return req;
    };

    auto missingDirectoryResult = WaitFor(makeStartRequest()->Invoke());
    ASSERT_FALSE(missingDirectoryResult.IsOK());
    EXPECT_TRUE(missingDirectoryResult.FindMatching(NNodeTrackerClient::EErrorCode::NoSuchNode));

    if (populateKnownTargets) {
        for (auto target : targets) {
            connection->GetNodeDirectory()->AddDescriptor(
                target.GetNodeId(),
                client->GetNativeConnection()->GetNodeDirectory()->GetDescriptor(target));
        }
    }

    auto startReq = makeStartRequest();
    if (!populateKnownTargets) {
        TNodeDirectoryBuilder nodeDirectoryBuilder(
            client->GetNativeConnection()->GetNodeDirectory(),
            startReq->mutable_node_directory());
        nodeDirectoryBuilder.Add(targets);
    }

    WaitFor(startReq->Invoke())
        .ThrowOnError();

    EXPECT_EQ(std::ssize(connection->GetNodeDirectory()->GetAllDescriptors()), 1 + std::ssize(targets));
    for (auto target : targets) {
        const auto* descriptor = connection->GetNodeDirectory()->FindDescriptor(target.GetNodeId());
        ASSERT_TRUE(descriptor) << ToString(target.GetNodeId());
        EXPECT_EQ(*descriptor, client->GetNativeConnection()->GetNodeDirectory()->GetDescriptor(target));
    }
    EXPECT_EQ(connection->GetNodeDirectory()->GetDescriptor(unrelatedNodeId).GetDefaultAddress(), "unrelated:1");

    auto writeReq = proxy.WriteRecord();
    ToProto(writeReq->mutable_session_id(), sessionId);
    const std::string payload = "payload";
    writeReq->mutable_statistics()->set_data_weight(std::ssize(payload));
    writeReq->mutable_statistics()->set_uncompressed_data_size(std::ssize(payload));
    writeReq->mutable_statistics()->set_row_count(1);
    writeReq->Attachments().push_back(TSharedRef::FromString(payload));
    WaitFor(writeReq->Invoke())
        .ThrowOnError();

    auto finishReq = proxy.FinishSession();
    ToProto(finishReq->mutable_session_id(), sessionId);
    WaitFor(finishReq->Invoke())
        .ThrowOnError();

    auto reader = NJournalClient::CreateChunkReader(
        New<NJournalClient::TChunkReaderConfig>(),
        New<TRemoteReaderOptions>(),
        New<TChunkReaderHost>(client),
        sessionId.ChunkId,
        NErasure::ECodec::None,
        TChunkReplicaWithMedium::ToChunkReplicas(targets));
    auto blocks = WaitFor(reader->ReadBlocks(IChunkReader::TReadBlocksOptions{}, {0}))
        .ValueOrThrow();

    ASSERT_EQ(std::ssize(blocks), 1);
    EXPECT_EQ(blocks[0].Data.ToStringBuf(), payload);

    WaitFor(transaction->Abort())
        .ThrowOnError();
}

INSTANTIATE_TEST_SUITE_P(
    TargetDescriptors,
    TDistributedChunkSessionNodeDirectoryTest,
    ::testing::Bool(),
    [] (const ::testing::TestParamInfo<bool>& info) {
        return std::string(info.param ? "KnownToSequencer" : "SuppliedInRequest");
    });

////////////////////////////////////////////////////////////////////////////////

class TChunkLocationService
    : public NRpc::TServiceBase
{
public:
    TChunkLocationService(IInvokerPtr invoker, TChunkId chunkId, TChunkReplicaList replicas)
        : TServiceBase(
            std::move(invoker),
            TChunkServiceProxy::GetDescriptor(),
            NLogging::TLogger("ChunkLocationTest"))
        , ChunkId_(chunkId)
        , Replicas_(std::move(replicas))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LocateChunks));
    }

private:
    const TChunkId ChunkId_;
    const TChunkReplicaList Replicas_;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, LocateChunks)
    {
        THROW_ERROR_EXCEPTION_IF(
            request->subrequests_size() != 1 || FromProto<TChunkId>(request->subrequests(0)) != ChunkId_,
            "Unexpected chunk location request");

        response->mutable_node_directory();
        auto* subresponse = response->add_subresponses();
        for (auto replica : Replicas_) {
            subresponse->add_replicas(ToProto<ui64>(TChunkReplicaWithMedium(
                replica.GetNodeId(),
                replica.GetReplicaIndex(),
                GenericMediumIndex)));
        }

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionReaderNodeDirectoryTest
    : public TApiTestBase
    , public ::testing::WithParamInterface<bool>
{ };

TEST_P(TDistributedChunkSessionReaderNodeDirectoryTest, ReportsResolutionFailure)
{
    const auto hasReplicas = GetParam();
    auto nativeClient = DynamicPointerCast<NNative::IClient>(Client_);
    auto connection = NNative::CreateConnection(nativeClient->GetNativeConnection()->GetCompoundConfig());
    auto terminateConnection = Finally([&] { connection->Terminate(); });

    auto chunkId = MakeRandomId(EObjectType::JournalChunk, connection->GetPrimaryMasterCellTag());
    TChunkReplicaList replicas = {TChunkReplica(TNodeId(1), GenericChunkReplicaIndex)};

    auto actionQueue = New<TActionQueue>("ReaderDirectoryTest");
    auto service = New<TChunkLocationService>(
        actionQueue->GetInvoker(),
        chunkId,
        hasReplicas ? replicas : TChunkReplicaList{});
    auto channelFactory = NRpc::CreateTestChannelFactoryWithDefaultServices(service);
    auto masterChannel = channelFactory->CreateChannel("local:master");
    auto clientOptions = NNative::TClientOptions::Root();
    clientOptions.ChannelWrapper = BIND([masterChannel] (NRpc::IChannelPtr /*channel*/) {
        return masterChannel;
    });

    auto client = connection->CreateNativeClient(clientOptions);

    auto config = New<TDistributedChunkSessionReaderConfig>();
    config->MaxReadAttempts = 2;
    config->ErrorBackoff.MinBackoff = TDuration::MilliSeconds(10);
    config->ErrorBackoff.MaxBackoff = TDuration::MilliSeconds(10);

    auto reader = CreateDistributedChunkSessionReader(
        config,
        client,
        New<TChunkReaderHost>(client),
        chunkId,
        replicas,
        /*readQuorum*/ 1,
        /*startRecordIndex*/ 0,
        /*rangeEndRecordIndex*/ std::nullopt,
        actionQueue->GetInvoker());

    auto result = WaitFor(reader->Read().WithTimeout(TDuration::Seconds(30)));

    ASSERT_FALSE(result.IsOK());
    EXPECT_EQ(reader->GetStatistics()->ErrorAttemptCount.load(), config->MaxReadAttempts);
    EXPECT_EQ(reader->GetStatistics()->MasterRefreshCount.load(), config->MaxReadAttempts);
    if (hasReplicas) {
        EXPECT_TRUE(result.FindMatching(NNodeTrackerClient::EErrorCode::NoSuchNode));
    } else {
        EXPECT_FALSE(result.FindMatching(NNodeTrackerClient::EErrorCode::NoSuchNode));
        EXPECT_THAT(ToString(result), ::testing::HasSubstr("no replicas"));
    }
}

INSTANTIATE_TEST_SUITE_P(
    MasterResponse,
    TDistributedChunkSessionReaderNodeDirectoryTest,
    ::testing::Bool(),
    [] (const ::testing::TestParamInfo<bool>& info) {
        return std::string(info.param ? "MissingDescriptor" : "NoReplicas");
    });

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
