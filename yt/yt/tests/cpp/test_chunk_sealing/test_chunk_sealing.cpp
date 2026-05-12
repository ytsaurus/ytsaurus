#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/journal_client/chunk_reader.h>
#include <yt/yt/ytlib/journal_client/journal_chunk_writer.h>
#include <yt/yt/ytlib/journal_client/journal_ypath_proxy.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/journal_client/config.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/journal_client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

#include <util/random/random.h>

namespace NYT {
namespace NCppTests {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NJournalClient;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTransactionClient;

using testing::HasSubstr;

using NChunkClient::NProto::TMiscExt;

using NJournalClient::TChunkReaderConfig;
using NJournalClient::CreateChunkReader;

////////////////////////////////////////////////////////////////////////////////

static const TLogger Logger("ChunkSealingTest");

////////////////////////////////////////////////////////////////////////////////

class TChunkSealingTest
    : public TApiTestBase
{
protected:
    NNative::IClientPtr NativeClient_;
    NNative::IConnectionPtr NativeConnection_;
    TActionQueuePtr ActionQueue_;
    ITransactionPtr Transaction_;

    static constexpr int ReplicationFactor = 3;
    static constexpr int WriteQuorum = 2;
    static constexpr int ReadQuorum = 2;

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

        ActionQueue_ = New<TActionQueue>("TestChunkSealing");

        Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();
    }

    struct TChunkWriteResult
    {
        TChunkId ChunkId;
        TChunkReplicaWithMediumList Replicas;
        std::vector<std::string> Records;
        i64 TotalDataSize = 0;
    };

    // Creates an unattached journal chunk on master, allocates write targets,
    // writes records via TJournalChunkWriter, and closes the writer.
    TChunkWriteResult CreateAndWriteJournalChunk(int recordCount, int recordSize)
    {
        // Create chunk on master.
        auto channel = NativeClient_->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader,
            CellTagFromId(Transaction_->GetId()));
        TChunkServiceProxy proxy(channel);

        auto createReq = proxy.CreateChunk();
        createReq->SetTimeout(TDuration::Seconds(30));
        GenerateMutationId(createReq);

        createReq->set_type(ToProto(EObjectType::JournalChunk));
        createReq->set_account("intermediate");
        ToProto(createReq->mutable_transaction_id(), Transaction_->GetId());
        createReq->set_replication_factor(ReplicationFactor);
        createReq->set_erasure_codec(ToProto(NErasure::ECodec::None));
        createReq->set_medium_name("default");
        createReq->set_read_quorum(ReadQuorum);
        createReq->set_write_quorum(WriteQuorum);
        createReq->set_movable(true);
        createReq->set_vital(true);

        auto sessionId = FromProto<TSessionId>(
            WaitFor(createReq->Invoke())
                .ValueOrThrow()
                ->session_id());

        YT_LOG_INFO("Created journal chunk (ChunkId: %v)", sessionId.ChunkId);

        // Allocate write targets.
        auto targets = AllocateWriteTargets(
            NativeClient_,
            sessionId,
            /*desiredTargetCount*/ ReplicationFactor,
            /*minTargetCount*/ ReplicationFactor,
            /*replicationFactorOverride*/ {},
            /*preferredHostName*/ {},
            /*forbiddenAddresses*/ {},
            /*allocatedAddresses*/ {},
            Logger);

        // Create and open journal chunk writer.
        auto writerOptions = New<TJournalChunkWriterOptions>();
        writerOptions->ReplicationFactor = ReplicationFactor;
        writerOptions->WriteQuorum = WriteQuorum;
        writerOptions->ReadQuorum = ReadQuorum;

        auto writerConfig = New<TJournalChunkWriterConfig>();

        auto writer = CreateJournalChunkWriter(
            NativeClient_,
            sessionId,
            writerOptions,
            writerConfig,
            /*counters*/ {},
            ActionQueue_->GetInvoker(),
            targets,
            EChunkFormat::JournalDistributed,
            Logger);

        WaitFor(writer->Open())
            .ThrowOnError();

        // Write records.
        TChunkWriteResult result;
        result.ChunkId = sessionId.ChunkId;
        result.Replicas = targets;
        result.Records.reserve(recordCount);

        for (int i = 0; i < recordCount; ++i) {
            auto record = MakeRandomString(recordSize);
            result.TotalDataSize += record.size();
            result.Records.push_back(record);
            WaitFor(writer->WriteRecord(TSharedRef::FromString(record)))
                .ThrowOnError();
        }

        // Close writer — waits for quorum flush.
        WaitFor(writer->Close())
            .ThrowOnError();

        YT_LOG_INFO("Chunk written and closed (ChunkId: %v, RecordCount: %v)",
            result.ChunkId,
            recordCount);

        return result;
    }

    void SealChunkWithInfo(TChunkId chunkId, i64 rowCount, i64 dataSize)
    {
        auto channel = NativeClient_->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader,
            CellTagFromId(chunkId));
        TChunkServiceProxy proxy(channel);

        auto req = proxy.SealChunk();
        req->SetTimeout(TDuration::Seconds(30));
        GenerateMutationId(req);
        ToProto(req->mutable_chunk_id(), chunkId);
        req->mutable_info()->set_row_count(rowCount);
        req->mutable_info()->set_uncompressed_data_size(dataSize);
        req->mutable_info()->set_compressed_data_size(dataSize);

        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    void ScheduleChunkSeal(TChunkId chunkId)
    {
        auto channel = NativeClient_->GetMasterChannelOrThrow(
            EMasterChannelKind::Leader,
            CellTagFromId(chunkId));
        TChunkServiceProxy proxy(channel);

        auto req = proxy.ScheduleChunkSeal();
        req->SetTimeout(TDuration::Seconds(30));
        GenerateMutationId(req);
        ToProto(req->mutable_chunk_id(), chunkId);

        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    i64 GetChunkDiskSpace(TChunkId chunkId)
    {
        auto diskSpace = WaitFor(Client_->GetNode(Format("#%v/@disk_space", chunkId)))
            .ValueOrThrow();
        return ConvertTo<i64>(diskSpace);
    }

    bool IsChunkSealed(TChunkId chunkId)
    {
        auto sealed = WaitFor(Client_->GetNode(Format("#%v/@sealed", chunkId)))
            .ValueOrThrow();
        return ConvertTo<bool>(sealed);
    }

    void WaitForChunkSealed(TChunkId chunkId)
    {
        // TChunkSealer runs AbortSessionsQuorum + ComputeQuorumInfo + SealChunk
        // mutation, which may take a while under CI load.
        WaitUntil(
            [&] {
                return IsChunkSealed(chunkId);
            },
            "Chunk is not sealed",
            {.Timeout = TDuration::Seconds(60)});
    }

    // Reads all records from a specific replica (not quorum).
    std::vector<std::string> ReadRecordsFromReplica(
        TChunkId chunkId,
        const std::string& address,
        int expectedRecordCount)
    {
        auto nodeDirectory = NativeConnection_->GetNodeDirectory();

        TNodeId nodeId;
        for (const auto& [id, descriptor] : nodeDirectory->GetAllDescriptors()) {
            if (descriptor.GetDefaultAddress() == address) {
                nodeId = id;
                break;
            }
        }
        YT_VERIFY(nodeId != TNodeId{});

        TChunkReplicaWithMedium replica(
            nodeId,
            /*replicaIndex*/ 0,
            /*mediumIndex*/ GenericMediumIndex);

        auto reader = CreateChunkReader(
            New<NJournalClient::TChunkReaderConfig>(),
            New<TRemoteReaderOptions>(),
            New<TChunkReaderHost>(NativeClient_),
            chunkId,
            NErasure::ECodec::None,
            /*replicas*/ {replica});

        auto blocks = WaitFor(reader->ReadBlocks(
            IChunkReader::TReadBlocksOptions{},
            /*firstBlockIndex*/ 0,
            /*blockCount*/ expectedRecordCount))
            .ValueOrThrow();

        std::vector<std::string> records;
        records.reserve(blocks.size());
        for (const auto& block : blocks) {
            records.push_back(std::string(block.Data.ToStringBuf()));
        }

        return records;
    }

    std::vector<std::string> GetChunkReplicaAddresses(TChunkId chunkId)
    {
        return ConvertTo<std::vector<std::string>>(
            WaitFor(Client_->GetNode(Format("#%v/@stored_replicas", chunkId)))
                .ValueOrThrow());
    }

    // Verifies that every replica of the chunk contains exactly the expected records.
    void VerifyAllReplicasContainRecords(
        TChunkId chunkId,
        const std::vector<std::string>& expectedRecords)
    {
        auto addresses = GetChunkReplicaAddresses(chunkId);

        ASSERT_GE(std::ssize(addresses), ReadQuorum)
            << "Not enough replicas for chunk " << ToString(chunkId);

        for (const auto& address : addresses) {
            auto records = ReadRecordsFromReplica(chunkId, address, expectedRecords.size());
            ASSERT_EQ(std::ssize(records), std::ssize(expectedRecords))
                << "Replica " << address << " has wrong record count for chunk " << ToString(chunkId);
            for (int i = 0; i < std::ssize(expectedRecords); ++i) {
                EXPECT_EQ(records[i], expectedRecords[i])
                    << "Replica " << address << " has wrong record at index " << i
                    << " for chunk " << ToString(chunkId);
            }
        }
    }

    static std::string MakeRandomString(int stringSize)
    {
        std::string randomString;
        randomString.reserve(stringSize);
        for (int i = 0; i < stringSize; ++i) {
            randomString += ('a' + RandomNumber<unsigned>() % 25);
        }
        return randomString;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TChunkSealingTest, HappyPathSealWithExactInfo)
{
    auto result = CreateAndWriteJournalChunk(/*recordCount*/ 10, /*recordSize*/ 100);

    ASSERT_FALSE(IsChunkSealed(result.ChunkId));
    ASSERT_EQ(GetChunkDiskSpace(result.ChunkId), 0);

    SealChunkWithInfo(
        result.ChunkId,
        /*rowCount*/ std::ssize(result.Records),
        /*dataSize*/ result.TotalDataSize);

    ASSERT_TRUE(IsChunkSealed(result.ChunkId));
    EXPECT_GT(GetChunkDiskSpace(result.ChunkId), 0);

    // After sealing with exact info, every replica must contain all records.
    VerifyAllReplicasContainRecords(result.ChunkId, result.Records);
}

TEST_F(TChunkSealingTest, FailurePathSealWithAbsentInfo)
{
    auto result = CreateAndWriteJournalChunk(/*recordCount*/ 10, /*recordSize*/ 100);

    ASSERT_FALSE(IsChunkSealed(result.ChunkId));
    ASSERT_EQ(GetChunkDiskSpace(result.ChunkId), 0);

    // Seal without providing info — master delegates to TChunkSealer.
    ScheduleChunkSeal(result.ChunkId);

    // TChunkSealer runs asynchronously; wait for the chunk to become sealed.
    WaitForChunkSealed(result.ChunkId);

    EXPECT_GT(GetChunkDiskSpace(result.ChunkId), 0);

    // After sealing via TChunkSealer, every replica must contain all records.
    VerifyAllReplicasContainRecords(result.ChunkId, result.Records);
}

TEST_F(TChunkSealingTest, FailurePathSealWithAbsentInfoMultipleRecords)
{
    // Write more records to test that TChunkSealer correctly computes row count.
    auto result = CreateAndWriteJournalChunk(/*recordCount*/ 50, /*recordSize*/ 200);

    ASSERT_FALSE(IsChunkSealed(result.ChunkId));

    ScheduleChunkSeal(result.ChunkId);

    WaitForChunkSealed(result.ChunkId);

    VerifyAllReplicasContainRecords(result.ChunkId, result.Records);
}

TEST_F(TChunkSealingTest, SealAlreadySealedChunkIsIdempotent)
{
    auto result = CreateAndWriteJournalChunk(/*recordCount*/ 5, /*recordSize*/ 100);

    SealChunkWithInfo(
        result.ChunkId,
        /*rowCount*/ std::ssize(result.Records),
        /*dataSize*/ result.TotalDataSize);

    ASSERT_TRUE(IsChunkSealed(result.ChunkId));

    // Sealing again with info should not fail.
    SealChunkWithInfo(
        result.ChunkId,
        /*rowCount*/ std::ssize(result.Records),
        /*dataSize*/ result.TotalDataSize);

    ASSERT_TRUE(IsChunkSealed(result.ChunkId));
}

TEST_F(TChunkSealingTest, ScheduleChunkSealAlreadySealedIsIdempotent)
{
    auto result = CreateAndWriteJournalChunk(/*recordCount*/ 5, /*recordSize*/ 100);

    SealChunkWithInfo(
        result.ChunkId,
        /*rowCount*/ std::ssize(result.Records),
        /*dataSize*/ result.TotalDataSize);

    ASSERT_TRUE(IsChunkSealed(result.ChunkId));

    // Sealing again without info should not fail.
    ScheduleChunkSeal(result.ChunkId);

    ASSERT_TRUE(IsChunkSealed(result.ChunkId));
}

TEST_F(TChunkSealingTest, ScheduleChunkSealThenSealWithExactInfo)
{
    auto result = CreateAndWriteJournalChunk(/*recordCount*/ 10, /*recordSize*/ 100);

    ASSERT_FALSE(IsChunkSealed(result.ChunkId));

    // Schedule seal (sets Sealable flag) but seal with exact info before
    // TChunkSealer runs. This can happen if the controller agent restarts
    // and recovers the session with exact stats.
    ScheduleChunkSeal(result.ChunkId);

    SealChunkWithInfo(
        result.ChunkId,
        /*rowCount*/ std::ssize(result.Records),
        /*dataSize*/ result.TotalDataSize);

    ASSERT_TRUE(IsChunkSealed(result.ChunkId));

    VerifyAllReplicasContainRecords(result.ChunkId, result.Records);
}

TEST_F(TChunkSealingTest, ScheduleChunkSealOnUnconfirmedChunkFails)
{
    // Create a journal chunk on master but do not confirm it (no writer Open).
    auto channel = NativeClient_->GetMasterChannelOrThrow(
        EMasterChannelKind::Leader,
        CellTagFromId(Transaction_->GetId()));
    TChunkServiceProxy proxy(channel);

    auto createReq = proxy.CreateChunk();
    createReq->SetTimeout(TDuration::Seconds(30));
    GenerateMutationId(createReq);
    createReq->set_type(ToProto(EObjectType::JournalChunk));
    createReq->set_account("intermediate");
    ToProto(createReq->mutable_transaction_id(), Transaction_->GetId());
    createReq->set_replication_factor(ReplicationFactor);
    createReq->set_erasure_codec(ToProto(NErasure::ECodec::None));
    createReq->set_medium_name("default");
    createReq->set_read_quorum(ReadQuorum);
    createReq->set_write_quorum(WriteQuorum);
    createReq->set_vital(true);

    auto chunkId = FromProto<TSessionId>(
        WaitFor(createReq->Invoke())
            .ValueOrThrow()
            ->session_id()).ChunkId;

    auto error = WaitFor(
        [&] {
            auto req = proxy.ScheduleChunkSeal();
            req->SetTimeout(TDuration::Seconds(30));
            GenerateMutationId(req);
            ToProto(req->mutable_chunk_id(), chunkId);
            return req->Invoke().AsVoid();
        }());

    EXPECT_FALSE(error.IsOK());
    EXPECT_THAT(error.GetMessage(), HasSubstr("not confirmed"));
}

TEST_F(TChunkSealingTest, ScheduleChunkSealOnNonJournalChunkFails)
{
    // Create a regular (non-journal) chunk on master.
    auto channel = NativeClient_->GetMasterChannelOrThrow(
        EMasterChannelKind::Leader,
        CellTagFromId(Transaction_->GetId()));
    TChunkServiceProxy proxy(channel);

    auto createReq = proxy.CreateChunk();
    createReq->SetTimeout(TDuration::Seconds(30));
    GenerateMutationId(createReq);
    createReq->set_type(ToProto(EObjectType::Chunk));
    createReq->set_account("intermediate");
    ToProto(createReq->mutable_transaction_id(), Transaction_->GetId());
    createReq->set_replication_factor(ReplicationFactor);
    createReq->set_erasure_codec(ToProto(NErasure::ECodec::None));
    createReq->set_medium_name("default");
    createReq->set_vital(true);

    auto chunkId = FromProto<TSessionId>(
        WaitFor(createReq->Invoke())
            .ValueOrThrow()
            ->session_id()).ChunkId;

    auto error = WaitFor(
        [&] {
            auto req = proxy.ScheduleChunkSeal();
            req->SetTimeout(TDuration::Seconds(30));
            GenerateMutationId(req);
            ToProto(req->mutable_chunk_id(), chunkId);
            return req->Invoke().AsVoid();
        }());

    EXPECT_FALSE(error.IsOK());
    EXPECT_THAT(error.GetMessage(), HasSubstr("not a journal chunk"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCppTests
} // namespace NYT
