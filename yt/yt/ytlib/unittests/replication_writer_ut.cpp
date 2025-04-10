#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/replication_writer.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/unittests/misc/test_connection.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/test_proxy_service.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/random.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/client/api/config.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NRpc;

using NYT::ToProto;
using NYT::FromProto;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::vector<TChecksum> BlocksToChecksums(const std::vector<TBlock>& blocks)
{
    std::vector<TChecksum> checksums;
    checksums.reserve(blocks.size());
    for (auto &block : blocks) {
        checksums.push_back(block.GetOrComputeChecksum());
    }
    return checksums;
}

TString GenerateRandomString(size_t size, TRandomGenerator* generator)
{
    TString result;
    result.reserve(size + sizeof(ui64));
    while (result.size() < size) {
        ui64 value = generator->Generate<ui64>();
        result += TStringBuf(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    result.resize(size);
    return result;
}

std::vector<TBlock> CreateBlocks(int count, TRandomGenerator* generator)
{
    std::vector<TBlock> blocks;
    blocks.reserve(count);

    for (int index = 0; index < count; index++) {
        int size = 10 + generator->Generate<ui32>() % 11;
        blocks.push_back(TBlock(TSharedRef::FromString(GenerateRandomString(size, generator))));
    }

    return blocks;
}

std::vector<TChecksum> ChecksumsFromHashMap(const THashMap<i32, TBlock>& blocks)
{
    std::vector<TChecksum> checksums(blocks.size());

    for (const auto &[blocksIndex, block] : blocks) {
        checksums[blocksIndex] = block.GetOrComputeChecksum();
    }

    return checksums;
}

////////////////////////////////////////////////////////////////////////////////

class TTestDataNodeService
    : public TServiceBase
{
public:
    explicit TTestDataNodeService(size_t throttledBlockCount,
        bool alwaysFail,
        IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            TDataNodeServiceProxy::GetDescriptor(),
            NLogging::TLogger("Test"))
        , ThrottledBlockCount_(throttledBlockCount)
        , AlwaysFail_(alwaysFail)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CancelChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProbePutBlocks));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PutBlocks));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FlushBlocks));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PingSession)
    {
        if (!SessionCanceled_) {
            if (!SessionId_.has_value() || *SessionId_ != FromProto<TSessionId>(request->session_id())) {
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::NoSuchSession,
                    "Session %v is invalid or expired",
                    request->session_id());
            }
        }

        if (UseProbePutBlocks_) {
            if (AlwaysFail_) {
                response->mutable_probe_put_blocks_state()->set_approved_cumulative_block_size(0);
                response->mutable_probe_put_blocks_state()->set_requested_cumulative_block_size(0);
            } else {
                response->mutable_probe_put_blocks_state()->set_approved_cumulative_block_size(MaxCumulativeBlockSize_);
                response->mutable_probe_put_blocks_state()->set_requested_cumulative_block_size(MaxCumulativeBlockSize_);
            }
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, StartChunk)
    {
        YT_VERIFY(!SessionId_.has_value());
        SessionId_ = FromProto<TSessionId>(request->session_id());
        SessionStarted_ = true;

        UseProbePutBlocks_ = request->use_probe_put_blocks();
        response->set_use_probe_put_blocks(request->use_probe_put_blocks());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, CancelChunk)
    {
        YT_VERIFY(SessionId_.has_value());
        YT_VERIFY(*SessionId_ == FromProto<TSessionId>(request->session_id()));
        SessionId_ = std::nullopt;
        SessionCanceled_ = true;

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FinishChunk)
    {
        YT_VERIFY(SessionId_.has_value());
        YT_VERIFY(*SessionId_ == FromProto<TSessionId>(request->session_id()));
        SessionId_ = std::nullopt;

        *response->mutable_chunk_info() = {};
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ProbePutBlocks)
    {
        YT_VERIFY(SessionId_.has_value());
        YT_VERIFY(*SessionId_ == FromProto<TSessionId>(request->session_id()));

        if (AlwaysFail_) {
            response->mutable_probe_put_blocks_state()->set_approved_cumulative_block_size(0);
        } else {
            MaxCumulativeBlockSize_ = std::max(MaxCumulativeBlockSize_, request->cumulative_block_size());
            response->mutable_probe_put_blocks_state()->set_approved_cumulative_block_size(request->cumulative_block_size());
        }
        response->mutable_probe_put_blocks_state()->set_requested_cumulative_block_size(request->cumulative_block_size());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, PutBlocks)
    {
        YT_VERIFY(SessionId_.has_value());
        YT_VERIFY(*SessionId_ == FromProto<TSessionId>(request->session_id()));

        auto chunkId = FromProto<TSessionId>(request->session_id()).ChunkId;
        int firstBlockIndex = request->first_block_index();
        int blockCount = std::ssize(request->Attachments());
        int lastBlockIndex = firstBlockIndex + blockCount - 1;
        bool populateCache = request->populate_cache();
        bool flushBlocks = request->flush_blocks();
        i64 cumulativeBlockSize = request->cumulative_block_size();

        if (UseProbePutBlocks_) {
            YT_VERIFY(MaxCumulativeBlockSize_ >= cumulativeBlockSize);
        }

        auto blocks = GetRpcAttachedBlocks(request, /*validateChecksums*/ false);

        context->SetRequestInfo(
            "ChunkId: %v, Blocks: %v, PopulateCache: %v, "
            "FlushBlocks: %v, CumulativeBlockSize: %v",
            chunkId,
            FormatBlocks(firstBlockIndex, lastBlockIndex),
            populateCache,
            flushBlocks,
            cumulativeBlockSize);

        ++PutBlocksCounter_;
        if (AlwaysFail_ || PutBlocksCounter_ % ThrottledBlockCount_ != 0) {
            context->Reply(TError(
                NChunkClient::EErrorCode::WriteThrottlingActive,
                "Write throttling active"));
            return;
        }

        for (size_t i = firstBlockIndex; i < firstBlockIndex + blocks.size(); i++) {
            YT_VERIFY(!LocalBlocks_.contains(i));
            YT_VERIFY(!IsBlockFlushed_.contains(i));

            LocalBlocks_[i] = blocks[i - firstBlockIndex];
            IsBlockFlushed_[i] = false;
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, SendBlocks)
    {
        YT_VERIFY(SessionId_.has_value());
        YT_VERIFY(*SessionId_ == FromProto<TSessionId>(request->session_id()));

        auto chunkId = FromProto<TSessionId>(request->session_id()).ChunkId;
        int firstBlockIndex = request->first_block_index();
        int blockCount = request->block_count();
        int lastBlockIndex = firstBlockIndex + blockCount - 1;
        i64 cumulativeBlockSize = request->cumulative_block_size();
        auto targetDescriptor = FromProto<NNodeTrackerClient::TNodeDescriptor>(request->target_descriptor());

        context->SetRequestInfo(
            "ChunkId: %v, Blocks: %v, CumulativeBlockSize: %v, Target: %v",
            chunkId,
            FormatBlocks(firstBlockIndex, lastBlockIndex),
            cumulativeBlockSize,
            targetDescriptor);

        if (AlwaysFail_) {
            context->Reply(TError(
                NChunkClient::EErrorCode::WriteThrottlingActive,
                "Write throttling active"));
            return;
        }

        YT_VERIFY(ChannelFactory_);
        auto channel = ChannelFactory_->CreateChannel(targetDescriptor.GetDefaultAddress());
        TDataNodeServiceProxy proxy(channel);
        auto req = proxy.PutBlocks();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        ToProto(req->mutable_session_id(), *SessionId_);
        req->set_first_block_index(firstBlockIndex);
        req->set_cumulative_block_size(cumulativeBlockSize);

        std::vector<TBlock> blocks;
        blocks.reserve(blockCount);
        for (int blockIndex = firstBlockIndex; blockIndex < firstBlockIndex + blockCount; ++blockIndex) {
            blocks.push_back(LocalBlocks_.at(blockIndex));
        }
        SetRpcAttachedBlocks(req, blocks);

        req->Invoke()
            .Subscribe(BIND([=] (const TDataNodeServiceProxy::TErrorOrRspPutBlocksPtr& errorOrRsp) {
                if (errorOrRsp.IsOK()) {
                    response->set_close_demanded(errorOrRsp.Value()->close_demanded());
                    context->Reply();
                } else {
                    context->Reply(TError(
                        NChunkClient::EErrorCode::SendBlocksFailed,
                        "Error putting blocks to %v",
                        targetDescriptor.GetDefaultAddress())
                        << errorOrRsp);
                }
            }));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, FlushBlocks)
    {
        YT_VERIFY(SessionId_.has_value());
        YT_VERIFY(*SessionId_ == FromProto<TSessionId>(request->session_id()));

        auto blockIndex = request->block_index();

        for (i32 i = 0; i <= blockIndex; i++) {
            YT_VERIFY(IsBlockFlushed_.contains(i));

            IsBlockFlushed_.at(i) = true;
        }

        context->Reply();
    }

    void SetChannelFactory(IChannelFactory* channelFactory)
    {
        ChannelFactory_ = channelFactory;
    }

    bool GetSessionCanceled() const
    {
        return SessionCanceled_;
    }

    std::vector<TChecksum> GetBlockChecksums() const
    {
        return ChecksumsFromHashMap(LocalBlocks_);
    }

    bool GetAllBlocksFlushed() const
    {
        return std::all_of(LocalBlocks_.begin(), LocalBlocks_.end(), [this](const auto& pair) {
            return IsBlockFlushed_.at(pair.first);
        });
    }

    bool GetSessionStarted() const
    {
        return SessionStarted_;
    }
private:
    size_t ThrottledBlockCount_;
    bool AlwaysFail_;

    size_t PutBlocksCounter_ = 0;

    std::optional<TSessionId> SessionId_;
    bool SessionStarted_ = false;
    bool SessionCanceled_ = false;
    bool UseProbePutBlocks_ = false;
    i64 MaxCumulativeBlockSize_ = 0;

    // Weak Pointer because of circular dependency
    IChannelFactory* ChannelFactory_ = nullptr;
    THashMap<i32, TBlock> LocalBlocks_;
    THashMap<i32, bool> IsBlockFlushed_;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriterTestCase
{
    bool UseProbePutBlocks = false;
    size_t ReplicationFactor = 3;
    size_t NodeCount = 3;
    size_t BlockCount = 10;
    size_t ThrottledBlockCount = 1;
    std::set<i32> ThrottlingNodes;
    std::set<i32> FailedNodes;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

class TReplicationWriterTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TWriterTestCase>
{
public:
    std::vector<NNodeTrackerClient::TNodeDescriptor> NodeDescriptors;
    std::vector<TIntrusivePtr<TTestDataNodeService>> Services;
    std::vector<TBlock> GeneratedBlocks;
    IChunkWriterPtr Writer;
    NApi::NNative::TTestConnectionPtr Connection;
    TActionQueuePtr ActionQueue;
    IInvokerPtr Invoker;
    TIntrusivePtr<NNodeTrackerClient::TNodeDirectory> NodeDirectory;
    INodeMemoryTrackerPtr MemoryTracker;

    void SetUp() override
    {
        auto testCase = GetParam();

        auto useProbePutBlocks = testCase.UseProbePutBlocks;
        auto replicationFactor = testCase.ReplicationFactor;
        auto blockCount = testCase.BlockCount;
        auto nodeCount = testCase.NodeCount;
        size_t throttledBlockCount = testCase.ThrottledBlockCount;
        std::set<i32> throttlingNodes = testCase.ThrottlingNodes;
        std::set<i32> failedNodes = testCase.FailedNodes;

        ActionQueue = New<TActionQueue>();
        Invoker = CreateSerializedInvoker(ActionQueue->GetInvoker());
        NodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        MemoryTracker = CreateNodeMemoryTracker(32_MB, {});

        NodeDescriptors.reserve(nodeCount);

        THashMap<std::string, IServicePtr> addressToService;

        auto chunkId = TGuid::Create();

        TRandomGenerator generator(42);
        GeneratedBlocks = CreateBlocks(blockCount, &generator);
        EXPECT_EQ(GeneratedBlocks.size(), blockCount);

        TChunkReplicaWithMediumList replicaList;

        for (size_t index = 0; index < nodeCount; ++index) {
            auto address = std::string(Format("local:%v", index));
            NodeDescriptors.push_back(NNodeTrackerClient::TNodeDescriptor(address));
            NodeDirectory->AddDescriptor(
                NNodeTrackerClient::TNodeId(index),
                NodeDescriptors.back());

            Services.push_back(New<TTestDataNodeService>(throttlingNodes.contains(index) ? throttledBlockCount : 1,
                    failedNodes.contains(index),
                    Invoker));

            addressToService[address] = Services.back();

            replicaList.push_back(TChunkReplicaWithMedium(NNodeTrackerClient::TNodeId(index), index, AllMediaIndex));
        }

        auto options = New<TRemoteWriterOptions>();
        options->MediumName = "test_medium";
        options->PlacementId = TGuid::Create();

        auto channelFactory = CreateTestChannelFactory(addressToService, THashMap<std::string, IServicePtr>());

        for (auto &service : Services) {
            service->SetChannelFactory(channelFactory.Get());
        }

        Connection = NApi::NNative::CreateConnection(
            std::move(channelFactory),
            {"default"},
            std::move(NodeDirectory),
            Invoker,
            MemoryTracker);

        EXPECT_CALL(*Connection, GetPrimaryMasterCellId).Times(testing::AtLeast(1));
        EXPECT_CALL(*Connection, GetClusterDirectory).Times(testing::AtLeast(1));
        EXPECT_CALL(*Connection, SubscribeReconfigured).Times(testing::AtLeast(1));
        EXPECT_CALL(*Connection, UnsubscribeReconfigured).Times(testing::AtLeast(1));
        EXPECT_CALL(*Connection, GetPrimaryMasterCellTag).Times(testing::AtLeast(1));
        EXPECT_CALL(*Connection, GetSecondaryMasterCellTags).Times(testing::AtLeast(1));

        auto client = Connection->CreateNativeClient(NApi::TClientOptions::FromUser("test_user"));

        auto config = New<TReplicationWriterConfig>();
        config->NodePingPeriod = TDuration::Seconds(1);
        config->ProbePutBlocksTimeout = TDuration::Seconds(5);
        config->UseProbePutBlocks = useProbePutBlocks;
        config->MinUploadReplicationFactor = replicationFactor;
        config->UploadReplicationFactor = nodeCount;
        config->NodeChannel = New<TRetryingChannelConfig>();
        config->NodeChannel->RetryAttempts = 5;

        TChunkReplicaWithMediumList replicasPerMedium;
        for (size_t node = 0; node < nodeCount; ++node) {
            replicasPerMedium.push_back(TChunkReplicaWithMedium(NNodeTrackerClient::TNodeId(node), GenericChunkReplicaIndex, AllMediaIndex));
        }

        Writer = CreateReplicationWriter(
            std::move(config),
            std::move(options),
            TSessionId{chunkId, 0},
            replicasPerMedium,
            client,
            "localhost:100");
    }

    void TearDown() override
    {
        ActionQueue->Shutdown();
        MemoryTracker->ClearTrackers();
        MemoryTracker = nullptr;
        NodeDirectory = nullptr;
        Invoker = nullptr;
        ActionQueue = nullptr;
        Connection = nullptr;
        Writer = nullptr;
        GeneratedBlocks.clear();
        Services.clear();
        NodeDescriptors.clear();
    }
};

TEST_P(TReplicationWriterTest, WriteTest)
{
    auto testCase = GetParam();

    std::set<i32> failedNodes = testCase.FailedNodes;

    IChunkWriter::TWriteBlocksOptions writeOptions;
    TWorkloadDescriptor workloadDescriptor;

    Writer->Open()
        .Apply(BIND([&] {
            EXPECT_TRUE(Writer->WriteBlocks(writeOptions, workloadDescriptor, GeneratedBlocks));
            return Writer->GetReadyEvent();
        }))
        .Apply(BIND([&] {
            auto deferredMeta = New<TDeferredChunkMeta>();
            deferredMeta->set_type(0);
            deferredMeta->set_format(0);
            *deferredMeta->mutable_extensions() = {};
            return Writer->Close({}, {}, deferredMeta);
        }))
        .Wait(TDuration::Seconds(120));

    auto blockChecksums = BlocksToChecksums(GeneratedBlocks);

    EXPECT_TRUE(std::all_of(Services.begin(), Services.end(), [] (auto service) { return service->GetSessionStarted(); }));

    for (size_t i = 0; i < Services.size(); ++i) {
        if (!failedNodes.contains(i)) {
            EXPECT_FALSE(Services[i]->GetSessionCanceled());
        }
    }

    for (size_t i = 0; i < Services.size(); ++i) {
        if (!failedNodes.contains(i)) {
            EXPECT_TRUE(Services[i]->GetAllBlocksFlushed());
        }
    }

    for (size_t i = 0; i < Services.size(); ++i) {
        if (!failedNodes.contains(i)) {
            EXPECT_EQ(Services[i]->GetBlockChecksums(), blockChecksums);
        }
    }
}

TEST_P(TReplicationWriterTest, CancelTest)
{
    IChunkWriter::TWriteBlocksOptions writeOptions;
    TWorkloadDescriptor workloadDescriptor;

    Writer->Open()
        .Apply(BIND([&] {
            EXPECT_TRUE(Writer->WriteBlocks(writeOptions, workloadDescriptor, GeneratedBlocks));
            return Writer->GetReadyEvent();
        }))
        .Apply(BIND([&] {
            return Writer->Cancel();
        }))
        .Wait(TDuration::Seconds(60));

    EXPECT_TRUE(std::all_of(Services.begin(), Services.end(), [] (auto service) { return service->GetSessionCanceled(); }));
}


INSTANTIATE_TEST_SUITE_P(
    TReplicationWriterTest,
    TReplicationWriterTest,
    ::testing::Values(
        TWriterTestCase{
            .ReplicationFactor = 1,
            .NodeCount = 1,
            .BlockCount = 1024,
        },
        TWriterTestCase{
            .ReplicationFactor = 2,
            .NodeCount = 2,
            .BlockCount = 1024,
        },
        TWriterTestCase{
            .ReplicationFactor = 3,
            .NodeCount = 3,
            .BlockCount = 1024,
        },
        TWriterTestCase{
            .UseProbePutBlocks = true,
            .ReplicationFactor = 3,
            .NodeCount = 3,
            .BlockCount = 1024,
        },
        TWriterTestCase{
            .ReplicationFactor = 4,
            .NodeCount = 4,
            .BlockCount = 1024,
        },
        TWriterTestCase{
            .ReplicationFactor = 5,
            .NodeCount = 5,
            .BlockCount = 1024,
        },
        TWriterTestCase{
            .ReplicationFactor = 3,
            .NodeCount = 3,
            .BlockCount = 1024,
            .ThrottledBlockCount = 2,
            .ThrottlingNodes = {1, 2},
        },
        TWriterTestCase{
            .ReplicationFactor = 3,
            .NodeCount = 3,
            .BlockCount = 1024,
            .ThrottledBlockCount = 5,
            .ThrottlingNodes = {0, 3},
        },
        TWriterTestCase{
            .UseProbePutBlocks = true,
            .ReplicationFactor = 3,
            .NodeCount = 5,
            .BlockCount = 1024,
            .FailedNodes = {0, 3},
        },
        TWriterTestCase{
            .UseProbePutBlocks = true,
            .ReplicationFactor = 2,
            .NodeCount = 5,
            .BlockCount = 1024,
            .FailedNodes = {2, 3, 4},
        }
        // ReplicationWriter doesn't handle failing nodes properly yet without probing
        // TWriterTestCase{
        //     .ReplicationFactor = 3,
        //     .NodeCount = 5,
        //     .BlockCount = 1024,
        //     .FailedNodes = {0, 3},
        // },
        // TWriterTestCase{
        //     .ReplicationFactor = 2,
        //     .NodeCount = 5,
        //     .BlockCount = 1024,
        //     .FailedNodes = {2, 3, 4},
        // }
    ));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
