#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/replication_writer.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/job_io_meter.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>
#include <yt/yt/ytlib/chunk_client/proto/chunk_writer_statistics.pb.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/test_framework/test_connection.h>

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
    for (const auto& block : blocks) {
        checksums.push_back(block.GetOrComputeChecksum());
    }
    return checksums;
}

std::string GenerateRandomString(int size, TRandomGenerator* generator)
{
    std::string result;
    result.reserve(size + sizeof(ui64));
    while (std::ssize(result) < size) {
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
        int size = 10 + generator->Generate<uint>() % 11;
        blocks.push_back(TBlock(TSharedRef::FromString(GenerateRandomString(size, generator))));
    }

    return blocks;
}

std::vector<TChecksum> ChecksumsFromHashMap(const THashMap<int, TBlock>& blocks)
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
    explicit TTestDataNodeService(int throttledBlockCount,
        bool alwaysFail,
        bool netThrottling,
        bool useErrorOnNetThrottling,
        IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            TDataNodeServiceProxy::GetDescriptor(),
            NLogging::TLogger("Test"))
        , ThrottledBlockCount_(throttledBlockCount)
        , AlwaysFail_(alwaysFail)
        , NetThrottling_(netThrottling)
        , UseErrorOnNetThrottling_(useErrorOnNetThrottling)
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
            auto sessionId = FromProto<TSessionId>(request->session_id());
            if (!SessionId_.has_value() || *SessionId_ != sessionId) {
                THROW_ERROR_EXCEPTION(
                    NChunkClient::EErrorCode::NoSuchSession,
                    "Session %v is invalid or expired",
                    sessionId);
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

        if (NetThrottling_) {
            response->set_net_throttling(true);
            response->set_net_queue_size(10_KB);
        } else {
            response->set_net_throttling(false);
            response->set_net_queue_size(0);
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

        UseSendBlocks_ = !request->disable_send_blocks();

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
        if (ReportChunkWriterStatistics_) {
            FillChunkWriterStatistics(response->mutable_chunk_writer_statistics(), /*dataBytes*/ 0, MetaBytesPerFinish_);
        }
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

        if (request->has_io_fair_share_weight()) {
            LastIoFairShareWeight_.store(request->io_fair_share_weight());
        } else {
            LastIoFairShareWeight_.store(-1);
        }

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

        for (int i = firstBlockIndex; i < firstBlockIndex + std::ssize(blocks); i++) {
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

        ++SendBlocksCounter_;

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

        if (NetThrottling_) {
            if (UseErrorOnNetThrottling_) {
                context->Reply(TError(
                    NChunkClient::EErrorCode::WriteThrottlingActive,
                    "Write throttling active"));
                return;
            } else {
                response->set_net_throttling(true);
                response->set_net_queue_size(10_KB);
                context->Reply();
            }
            return;
        }

        auto channelFactory = ChannelFactory_.Lock();
        YT_VERIFY(channelFactory);
        auto channel = channelFactory->CreateChannel(targetDescriptor.GetDefaultAddress());
        TDataNodeServiceProxy proxy(channel);
        auto req = proxy.PutBlocks();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        ToProto(req->mutable_session_id(), *SessionId_);
        req->set_first_block_index(firstBlockIndex);
        req->set_cumulative_block_size(cumulativeBlockSize);
        if (request->has_io_fair_share_weight()) {
            req->set_io_fair_share_weight(request->io_fair_share_weight());
        }

        std::vector<TBlock> blocks;
        blocks.reserve(blockCount);
        for (int blockIndex = firstBlockIndex; blockIndex < firstBlockIndex + blockCount; ++blockIndex) {
            auto it = LocalBlocks_.find(blockIndex);
            YT_VERIFY(it != LocalBlocks_.end());
            blocks.push_back(it->second);
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

        for (int i = 0; i <= blockIndex; i++) {
            auto it = IsBlockFlushed_.find(i);
            YT_VERIFY(it != IsBlockFlushed_.end());

            it->second = true;
        }

        if (ReportChunkWriterStatistics_) {
            FillChunkWriterStatistics(response->mutable_chunk_writer_statistics(), DataBytesPerFlush_, /*metaBytes*/ 0);
        }

        context->Reply();
    }

    void SetChannelFactory(TIntrusivePtr<IChannelFactory> channelFactory)
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
        return std::all_of(LocalBlocks_.begin(), LocalBlocks_.end(), [this] (const auto& pair) {
            auto it = IsBlockFlushed_.find(pair.first);
            YT_VERIFY(it != IsBlockFlushed_.end());
            return it->second;
        });
    }

    bool GetSessionStarted() const
    {
        return SessionStarted_;
    }

    bool GetUseSendBlocks() const
    {
        return UseSendBlocks_;
    }

    int GetSendBlocksCount() const
    {
        return SendBlocksCounter_;
    }

    void SetReportChunkWriterStatistics(i64 dataBytesPerFlush, i64 metaBytesPerFinish)
    {
        ReportChunkWriterStatistics_ = true;
        DataBytesPerFlush_ = dataBytesPerFlush;
        MetaBytesPerFinish_ = metaBytesPerFinish;
    }

    i64 GetReportedWriterBytes() const
    {
        return ReportedWriterBytes_.load();
    }

    double GetLastIoFairShareWeight() const
    {
        return LastIoFairShareWeight_.load();
    }

private:
    const int ThrottledBlockCount_;
    const bool AlwaysFail_;
    const bool NetThrottling_;

    int PutBlocksCounter_ = 0;
    int SendBlocksCounter_ = 0;
    bool UseSendBlocks_ = true;

    std::optional<TSessionId> SessionId_;
    bool SessionStarted_ = false;
    bool SessionCanceled_ = false;
    bool UseProbePutBlocks_ = false;
    bool UseErrorOnNetThrottling_ = true;
    i64 MaxCumulativeBlockSize_ = 0;

    bool ReportChunkWriterStatistics_ = false;
    i64 DataBytesPerFlush_ = 0;
    i64 MetaBytesPerFinish_ = 0;
    std::atomic<i64> ReportedWriterBytes_ = 0;
    std::atomic<double> LastIoFairShareWeight_ = -1;

    // Weak Pointer because of circular dependency
    TWeakPtr<IChannelFactory> ChannelFactory_ = nullptr;
    THashMap<int, TBlock> LocalBlocks_;
    THashMap<int, bool> IsBlockFlushed_;

    void FillChunkWriterStatistics(NProto::TChunkWriterStatistics* statistics, i64 dataBytes, i64 metaBytes)
    {
        statistics->set_data_bytes_written_to_disk(dataBytes);
        statistics->set_data_io_write_requests(0);
        statistics->set_meta_bytes_written_to_disk(metaBytes);
        statistics->set_meta_io_write_requests(0);
        statistics->set_meta_io_sync_requests(0);
        ReportedWriterBytes_.fetch_add(dataBytes + metaBytes);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TWriterTestCase
{
    bool UseProbePutBlocks = false;
    bool UseSendBlocks = true;
    int ReplicationFactor = 3;
    int NodeCount = 3;
    int BlockCount = 10;
    int ThrottledBlockCount = 1;
    std::set<int> NetThrottlingNodes;
    std::set<int> ThrottlingNodes;
    std::set<int> FailedNodes;
    std::optional<double> IoFairShareWeight;
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
    TTestConnectionPtr Connection;
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
        int throttledBlockCount = testCase.ThrottledBlockCount;
        std::set<int> netThrottlingNodes = testCase.NetThrottlingNodes;
        std::set<int> throttlingNodes = testCase.ThrottlingNodes;
        std::set<int> failedNodes = testCase.FailedNodes;

        ActionQueue = New<TActionQueue>();
        Invoker = CreateSerializedInvoker(ActionQueue->GetInvoker());
        NodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        MemoryTracker = CreateNodeMemoryTracker(32_MB, New<TNodeMemoryTrackerConfig>(), {});

        NodeDescriptors.reserve(nodeCount);

        THashMap<std::string, IServicePtr> addressToService;

        auto chunkId = NObjectClient::MakeRandomId(NObjectClient::EObjectType::Chunk, NObjectClient::TCellTag(0xf003));

        TRandomGenerator generator(42);
        GeneratedBlocks = CreateBlocks(blockCount, &generator);
        EXPECT_EQ(std::ssize(GeneratedBlocks), blockCount);

        TChunkReplicaWithMediumList replicaList;

        for (int index = 0; index < nodeCount; ++index) {
            auto address = std::string(Format("local:%v", index));
            NodeDescriptors.push_back(NNodeTrackerClient::TNodeDescriptor(address));
            NodeDirectory->AddDescriptor(
                NNodeTrackerClient::TNodeId(index),
                NodeDescriptors.back());

            Services.push_back(New<TTestDataNodeService>(throttlingNodes.contains(index) ? throttledBlockCount : 1,
                    failedNodes.contains(index),
                    netThrottlingNodes.contains(index),
                    false,
                    Invoker));

            addressToService[address] = Services.back();

            replicaList.push_back(TChunkReplicaWithMedium(NNodeTrackerClient::TNodeId(index), index, AllMediaIndex));
        }

        auto options = New<TRemoteWriterOptions>();
        options->MediumName = "test_medium";
        options->PlacementId = TGuid::Create();

        auto channelFactory = CreateTestChannelFactory(addressToService, THashMap<std::string, IServicePtr>());

        for (const auto& service : Services) {
            service->SetChannelFactory(channelFactory.Get());
        }

        Connection = CreateConnection(
            std::move(channelFactory),
            {"default"},
            NodeDirectory,
            /*nodeStatusDirectory*/ nullptr,
            Invoker,
            MemoryTracker);

        EXPECT_CALL(*Connection, CreateNativeClient).WillRepeatedly([this] (const NApi::NNative::TClientOptions& options) -> NApi::NNative::IClientPtr
            {
                return New<NApi::NNative::TClient>(Connection, options, MemoryTracker);
            });
        EXPECT_CALL(*Connection, GetPrimaryMasterCellId).Times(testing::AtLeast(1));
        EXPECT_CALL(*Connection, GetClusterDirectory).Times(testing::AtLeast(1));
        EXPECT_CALL(*Connection, SubscribeReconfigured).Times(testing::AtLeast(1));
        EXPECT_CALL(*Connection, UnsubscribeReconfigured).Times(testing::AtLeast(1));
        EXPECT_CALL(*Connection, GetPrimaryMasterCellTag).Times(testing::AtLeast(1));
        EXPECT_CALL(*Connection, GetSecondaryMasterCellTags).Times(testing::AtLeast(1));

        auto client = Connection->CreateNativeClient(NApi::NNative::TClientOptions::FromUser("test_user"));

        auto config = New<TReplicationWriterConfig>();
        config->NodePingPeriod = TDuration::Seconds(1);
        config->ProbePutBlocksTimeout = TDuration::Seconds(5);
        config->UseProbePutBlocks = useProbePutBlocks;
        config->MinUploadReplicationFactor = replicationFactor;
        config->UploadReplicationFactor = nodeCount;
        config->UseSendBlocks = testCase.UseSendBlocks;
        config->IoFairShareWeight = testCase.IoFairShareWeight;
        config->NodeChannel = New<TRetryingChannelConfig>();
        config->NodeChannel->RetryAttempts = 5;

        TChunkReplicaWithMediumList replicasPerMedium;
        for (int node = 0; node < nodeCount; ++node) {
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

    std::set<int> failedNodes = testCase.FailedNodes;

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
        .BlockingWait(TDuration::Seconds(120));

    auto blockChecksums = BlocksToChecksums(GeneratedBlocks);

    EXPECT_TRUE(std::all_of(Services.begin(), Services.end(), [] (auto service) { return service->GetSessionStarted(); }));

    for (int i = 0; i < std::ssize(Services); ++i) {
        if (!failedNodes.contains(i)) {
            EXPECT_FALSE(Services[i]->GetSessionCanceled());
        }
    }

    for (int i = 0; i < std::ssize(Services); ++i) {
        if (!failedNodes.contains(i)) {
            EXPECT_TRUE(Services[i]->GetAllBlocksFlushed());
        }
    }

    for (int i = 0; i < std::ssize(Services); ++i) {
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
        .BlockingWait(TDuration::Seconds(60));

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
        },
        TWriterTestCase{
            .ReplicationFactor = 3,
            .NodeCount = 3,
            .BlockCount = 1024,
            .NetThrottlingNodes = {0, 1, 2},
        },
        TWriterTestCase{
            .ReplicationFactor = 5,
            .NodeCount = 5,
            .BlockCount = 1024,
            .NetThrottlingNodes = {0, 1, 2},
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

class TUseSendBlocksWriterTest
    : public TReplicationWriterTest
{ };

TEST_P(TUseSendBlocksWriterTest, WriteTest)
{
    auto blockChecksums = BlocksToChecksums(GeneratedBlocks);

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
        .BlockingWait(TDuration::Seconds(120));

    for (const auto& service : Services) {
        EXPECT_FALSE(service->GetUseSendBlocks())
            << "use_send_blocks=false must be propagated to every node session via StartChunk RPC";
        EXPECT_EQ(service->GetSendBlocksCount(), 0)
            << "SendBlocks must not be called when UseSendBlocks is false";
        EXPECT_EQ(service->GetBlockChecksums(), blockChecksums)
            << "all blocks must be delivered correctly via PutBlocks";
    }
}

INSTANTIATE_TEST_SUITE_P(
    TUseSendBlocksWriterTest,
    TUseSendBlocksWriterTest,
    ::testing::Values(
        TWriterTestCase{
            .UseSendBlocks = false,
            .ReplicationFactor = 3,
            .NodeCount = 3,
            .BlockCount = 1024,
        },
        TWriterTestCase{
            .UseSendBlocks = false,
            .ReplicationFactor = 1,
            .NodeCount = 1,
            .BlockCount = 1024,
        }
    ));

////////////////////////////////////////////////////////////////////////////////

class TJobIoMeterWriterTest
    : public TReplicationWriterTest
{ };

TEST_P(TJobIoMeterWriterTest, AccountsWrittenBytes)
{
    constexpr i64 DataBytesPerFlush = 100;
    constexpr i64 MetaBytesPerFinish = 50;

    for (const auto& service : Services) {
        service->SetReportChunkWriterStatistics(DataBytesPerFlush, MetaBytesPerFinish);
    }

    auto jobIoMeter = New<TJobIoMeter>(TDuration::Hours(1));

    IChunkWriter::TWriteBlocksOptions writeOptions;
    writeOptions.ClientOptions.JobIoMeter = jobIoMeter;
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
            return Writer->Close(writeOptions, {}, deferredMeta);
        }))
        .BlockingWait(TDuration::Seconds(120));

    // The meter must accumulate exactly the disk-write bytes reported by the data
    // nodes across all FlushBlocks (data) and FinishChunk (meta) responses.
    i64 expectedBytes = 0;
    for (const auto& service : Services) {
        expectedBytes += service->GetReportedWriterBytes();
    }

    EXPECT_GT(expectedBytes, 0);
    EXPECT_EQ(jobIoMeter->GetIoConsumedInWindow(TDuration::Hours(1)), expectedBytes);
}

INSTANTIATE_TEST_SUITE_P(
    TJobIoMeterWriterTest,
    TJobIoMeterWriterTest,
    ::testing::Values(
        TWriterTestCase{
            .ReplicationFactor = 1,
            .NodeCount = 1,
            .BlockCount = 16,
        },
        TWriterTestCase{
            .ReplicationFactor = 3,
            .NodeCount = 3,
            .BlockCount = 16,
        }));

////////////////////////////////////////////////////////////////////////////////

class TIoFairShareWeightWriterTest
    : public TReplicationWriterTest
{ };

TEST_P(TIoFairShareWeightWriterTest, ReportsIoFairShareWeight)
{
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
            return Writer->Close(writeOptions, {}, deferredMeta);
        }))
        .BlockingWait(TDuration::Seconds(120));

    for (const auto& service : Services) {
        EXPECT_EQ(service->GetLastIoFairShareWeight(), 2.5);
    }
}

INSTANTIATE_TEST_SUITE_P(
    TIoFairShareWeightWriterTest,
    TIoFairShareWeightWriterTest,
    ::testing::Values(
        TWriterTestCase{
            .ReplicationFactor = 1,
            .NodeCount = 1,
            .BlockCount = 16,
            .IoFairShareWeight = 2.5,
        },
        TWriterTestCase{
            .ReplicationFactor = 3,
            .NodeCount = 3,
            .BlockCount = 16,
            .IoFairShareWeight = 2.5,
        }));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
