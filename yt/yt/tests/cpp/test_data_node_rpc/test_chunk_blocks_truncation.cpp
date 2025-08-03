#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/format.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <util/generic/xrange.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NYTree;

using NChunkClient::NProto::TBlocksExt;
using NChunkClient::NProto::TChunkInfo;
using NChunkClient::NProto::TChunkMeta;

using NChunkClient::EErrorCode;

using testing::Values;
using testing::HasSubstr;

std::mt19937 Gen(42);

////////////////////////////////////////////////////////////////////////////////

struct TChunkBlocksTruncationTestParams
{
    int BlockCount{};
    int BlockCountAfterTruncation{};
};

class TChunkBlocksTruncationTest
    : public TApiTestBase
    , public ::testing::WithParamInterface<TChunkBlocksTruncationTestParams>
{
protected:
    NNative::IClientPtr NativeClient_;
    ITransactionPtr Transaction_;
    TSessionId SessionId_;
    TMultiChunkWriterOptionsPtr ChunkWriterOptions_;
    TChunkReplicaWithMedium Target_;
    IChannelPtr Channel_;
    std::optional<TDataNodeServiceProxy> Proxy_;
    std::string NodeAddress_;

    void SetUp() override
    {
        NativeClient_ = DynamicPointerCast<NNative::IClient>(Client_);
        Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();

        ChunkWriterOptions_ = New<TMultiChunkWriterOptions>();
        ChunkWriterOptions_->Account = "intermediate";
        ChunkWriterOptions_->ReplicationFactor = 1;

        SessionId_ = CreateChunk(
            NativeClient_,
            CellTagFromId(Transaction_->GetId()),
            ChunkWriterOptions_,
            Transaction_->GetId(),
            TChunkListId{},
            TLogger());

        auto targets = AllocateWriteTargets(
            NativeClient_,
            TSessionId(DecodeChunkId(SessionId_.ChunkId).Id, SessionId_.MediumIndex),
            /*desiredTargetCount*/ ChunkWriterOptions_->ReplicationFactor,
            /*minTargetCount*/ ChunkWriterOptions_->ReplicationFactor);

        ASSERT_EQ(std::ssize(targets), 1);
        Target_ = std::move(targets.front());

        const auto& nodeDirectory = NativeClient_->GetNativeConnection()->GetNodeDirectory();
        const auto& channelFactory = NativeClient_->GetChannelFactory();
        const auto& networks = NativeClient_->GetNativeConnection()->GetNetworks();

        NodeAddress_ = nodeDirectory->GetDescriptor(Target_).GetAddressOrThrow(networks);
        Channel_ = channelFactory->CreateChannel(NodeAddress_);

        Proxy_.emplace(Channel_);
    }

    TDataNodeServiceProxy::TRspStartChunkPtr StartChunk()
    {
        auto req = Proxy_->StartChunk();
        ToProto(req->mutable_session_id(), SessionId_);
        req->set_sync_on_close(true);
        req->set_disable_send_blocks(true);
        SetRequestWorkloadDescriptor(req, TWorkloadDescriptor(EWorkloadCategory::UserBatch));

        return WaitFor(req->Invoke())
            .ValueOrThrow();
    }

    void PutBlocks(const std::vector<TBlock>& blocks)
    {
        int sentBlocksCount = 0;
        while (sentBlocksCount < std::ssize(blocks)) {
            int batchSize = std::uniform_int_distribution<int>(0, 8 * (std::ssize(blocks) - sentBlocksCount))(Gen);
            batchSize = std::min<int>(batchSize, std::ssize(blocks) - sentBlocksCount);

            auto req = Proxy_->PutBlocks();
            req->SetResponseHeavy(true);
            req->SetMultiplexingBand(EMultiplexingBand::Heavy);
            ToProto(req->mutable_session_id(), SessionId_);
            req->set_first_block_index(sentBlocksCount);
            req->set_flush_blocks(true);

            SetRpcAttachedBlocks(req, std::vector(blocks.begin() + sentBlocksCount, blocks.begin() + sentBlocksCount + batchSize));

            WaitFor(req->Invoke())
                .ThrowOnError();

            sentBlocksCount += batchSize;
        }
    }

    TDataNodeServiceProxy::TRspFinishChunkPtr FinishChunk(int blockCountAfterTruncation)
    {
        auto req = Proxy_->FinishChunk();
        ToProto(req->mutable_session_id(), SessionId_);
        req->set_block_count(blockCountAfterTruncation);
        req->mutable_chunk_meta()->set_type(ToProto(EChunkType::File));
        req->mutable_chunk_meta()->set_format(ToProto(EChunkFormat::FileDefault));
        req->mutable_chunk_meta()->mutable_extensions();
        req->set_truncate_extra_blocks(true);

        return WaitFor(req->Invoke())
            .ValueOrThrow();
    }

    TChunkMeta GetChunkMeta()
    {
        auto req = Proxy_->GetChunkMeta();
        ToProto(req->mutable_chunk_id(), SessionId_.ChunkId);
        req->set_all_extension_tags(true);

        auto res = WaitFor(req->Invoke())
            .ValueOrThrow();

        return res->chunk_meta();
    }

    std::vector<TBlock> GetBlockRange(int blockCount)
    {
        std::vector<TBlock> result;
        result.reserve(blockCount);
        while (std::ssize(result) < blockCount) {
            auto sendRequest = [&] (const auto& req) {
                ToProto(req->mutable_chunk_id(), SessionId_.ChunkId);
                SetRequestWorkloadDescriptor(req, TWorkloadDescriptor(EWorkloadCategory::UserBatch));

                auto res = WaitFor(req->Invoke())
                    .ValueOrThrow();

                auto blocks = GetRpcAttachedBlocks(res);
                for (auto& block : blocks) {
                    result.push_back(std::move(block));
                }
            };

            if (std::uniform_int_distribution(0, 1)(Gen) == 0) {
                auto req = Proxy_->GetBlockRange();
                req->set_first_block_index(std::ssize(result));
                req->set_block_count(blockCount - std::ssize(result));
                sendRequest(req);
            } else {
                auto req = Proxy_->GetBlockSet();
                for (int index = std::ssize(result); index < blockCount; ++index) {
                    req->add_block_indexes(index);
                }
                sendRequest(req);
            }
        }
        return result;
    }

    static TBlock MakeRandomBlock(i64 size)
    {
        auto data = TSharedMutableRef::Allocate(size, {.InitializeStorage = false});
        for (i64 index : xrange(size)) {
            data[index] = std::uniform_int_distribution<ui8>()(Gen);
        }
        return TBlock(std::move(data));
    }

    void ConfirmChunk(
        const TChunkInfo& chunkInfo,
        const TChunkMeta& chunkMeta,
        TChunkLocationUuid targetLocationUuid)
    {
        auto channel = NativeClient_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, CellTagFromId(SessionId_.ChunkId));
        TChunkServiceProxy proxy(channel);

        auto req = proxy.ConfirmChunk();
        GenerateMutationId(req);
        ToProto(req->mutable_chunk_id(), SessionId_.ChunkId);
        *req->mutable_chunk_info() = chunkInfo;
        *req->mutable_chunk_meta() = chunkMeta;

        req->set_location_uuids_supported(true);
        auto* replicaInfo = req->add_replicas();
        replicaInfo->set_replica(ToProto(Target_));
        ToProto(replicaInfo->mutable_location_uuid(), targetLocationUuid);
    }

    static i64 GetLocationUsedSpace(TChunkLocationUuid locationUuid)
    {
        auto statistics = ConvertToNode(
            WaitFor(Client_->GetNode(Format("//sys/chunk_locations/%v/@statistics", locationUuid)))
            .ValueOrThrow());
        return statistics->AsMap()->GetChildValueOrThrow<i64>("used_space");
    }
};

TEST_P(TChunkBlocksTruncationTest, BlocksTruncation)
{
    constexpr i64 maxBlockSize = 40_MB;

    auto params = GetParam();

    auto locationUuid = FromProto<TChunkLocationUuid>(StartChunk()->location_uuid());

    std::vector<TBlock> blocks;
    blocks.reserve(params.BlockCount);

    i64 totalBlocksSize = 0;
    for (int _ : xrange(params.BlockCount)) {
        blocks.push_back(MakeRandomBlock(std::uniform_int_distribution<i64>(0, maxBlockSize)(Gen)));
        totalBlocksSize += blocks.back().Size();
    }
    PutBlocks(blocks);

    if (params.BlockCountAfterTruncation > params.BlockCount) {
        EXPECT_THROW_THAT(
            FinishChunk(params.BlockCountAfterTruncation),
            HasSubstr("count mismatch"));
        return;
    }

    auto chunkStatistics = FinishChunk(params.BlockCountAfterTruncation);

    ConfirmChunk(chunkStatistics->chunk_info(), TChunkMeta{}, locationUuid);

    auto chunkMeta = GetChunkMeta();

    i64 truncatedBlocksSize = 0;
    for (int index : xrange(params.BlockCountAfterTruncation)) {
        truncatedBlocksSize += blocks[index].Size();
    }

    i64 metaDataSize = SerializeProtoToRefWithEnvelope(chunkMeta).Size();
    i64 headerSize = sizeof(TChunkMetaHeader_2);

    i64 expectedDiskSpace = truncatedBlocksSize + metaDataSize + headerSize;

    EXPECT_EQ(chunkStatistics->chunk_info().disk_space(), expectedDiskSpace);

    {
        auto blocksExt = GetProtoExtension<TBlocksExt>(chunkMeta.extensions());
        ASSERT_EQ(blocksExt.blocks_size(), params.BlockCountAfterTruncation);
    }

    {
        auto receivedBlocks = GetBlockRange(params.BlockCountAfterTruncation);
        ASSERT_EQ(std::ssize(receivedBlocks), params.BlockCountAfterTruncation);
        for (int index : xrange(params.BlockCountAfterTruncation)) {
            EXPECT_EQ(receivedBlocks[index].Data.ToStringBuf(), blocks[index].Data.ToStringBuf());
        }
    }

    {
        int extra = std::uniform_int_distribution(params.BlockCountAfterTruncation, params.BlockCount)(Gen) + 1;
        EXPECT_THROW_WITH_ERROR_CODE(
            GetBlockRange(params.BlockCountAfterTruncation + extra),
            EErrorCode::MalformedReadRequest);
    }

    {
        const auto& writerStatistics = chunkStatistics->chunk_writer_statistics();
        EXPECT_EQ(writerStatistics.data_bytes_written_to_disk(), totalBlocksSize);
        EXPECT_EQ(writerStatistics.meta_bytes_written_to_disk(), metaDataSize + headerSize);
    }

    {
        auto statistics = ConvertToNode(
            WaitFor(Client_->GetNode(Format("//sys/data_nodes/%v/orchid/data_node/stored_chunks/%v",
                NodeAddress_,
                SessionId_.ChunkId)))
            .ValueOrThrow());

        EXPECT_EQ(statistics->AsMap()->GetChildValueOrThrow<i64>("block_count"), params.BlockCountAfterTruncation);
        EXPECT_EQ(statistics->AsMap()->GetChildValueOrThrow<i64>("disk_space"), expectedDiskSpace);
    }

    {
        auto start = Now();
        // Wait for node heartbeat to master.
        while (GetLocationUsedSpace(locationUuid) != expectedDiskSpace && Now() - start < TDuration::Seconds(20)) {
            Sleep(TDuration::MilliSeconds(200));
        }
        EXPECT_EQ(GetLocationUsedSpace(locationUuid), expectedDiskSpace);
    }
}

INSTANTIATE_TEST_SUITE_P(
    TChunkBlocksTruncationTest,
    TChunkBlocksTruncationTest,
    Values(
        TChunkBlocksTruncationTestParams{100, 0},
        TChunkBlocksTruncationTestParams{100, 1},
        TChunkBlocksTruncationTestParams{100, 99},
        TChunkBlocksTruncationTestParams{100, 100},
        TChunkBlocksTruncationTestParams{42, 5},
        TChunkBlocksTruncationTestParams{5, 5},
        TChunkBlocksTruncationTestParams{0, 0},
        TChunkBlocksTruncationTestParams{1, 1},
        TChunkBlocksTruncationTestParams{1, 0},
        TChunkBlocksTruncationTestParams{10, 11} // In this case error should be thrown.
    )
);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests

