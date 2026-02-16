#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/node_tracker_client/node_status_directory.h>

#include <yt/yt/ytlib/test_framework/test_connection.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/test_proxy_service.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/bus/public.h>
#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/random.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/static_channel_factory.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NRpc;
using namespace NNodeTrackerClient;

using NYT::ToProto;
using NYT::FromProto;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestNodeStatusDirectory
    : public INodeStatusDirectory
{
public:
    void UpdateSuspicionMarkTime(
        TNodeId /*nodeId*/,
        TStringBuf /*address*/,
        bool /*suspicious*/,
        std::optional<TInstant> /*previousMarkTime*/) override
    { }

    std::vector<std::optional<TInstant>> RetrieveSuspicionMarkTimes(
        const std::vector<TNodeId>& nodeIds) const override
    {
        auto suspiciousNodeCount = SuspiciousNodeCount_.exchange(0);

        std::vector<std::optional<TInstant>> result(nodeIds.size());
        for (int index = 0; index < std::min<int>(suspiciousNodeCount, std::ssize(nodeIds)); ++index) {
            result[index] = TInstant::Now();
        }

        return result;
    }

    THashMap<TNodeId, TInstant> RetrieveSuspiciousNodeIdsWithMarkTime(
        const std::vector<TNodeId>& /*nodeIds*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    bool ShouldMarkNodeSuspicious(const TError& /*error*/) const override
    {
        YT_ABORT();
    }

    void SetSuspicousNodeCount(int suspiciousNodeCount)
    {
        SuspiciousNodeCount_ = suspiciousNodeCount;
    }

private:
    mutable std::atomic<int> SuspiciousNodeCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TProbeStatistics
{
    int DiskQueueSize = 0;
    int NetQueueSize = 0;
};

struct TTestCase
{
    int BatchCount = 1;
    int NodeCount = 3;
    int BlockCount = 10;
    int RequestInBatch = 10;
    int BlockInRequest = 2;
    bool Sequentially = true;
    bool EnableChunkProber = false;
    bool PartiallyThrottling = false;
    bool PartiallyBanns = false;
    std::optional<i64> BlockSetSubrequestThreshold;
    std::vector<std::pair<int, TDuration>> PartialPeerProbingTimeouts;
    bool MarkSomeNodesSuspicious = false;
    std::optional<std::vector<TProbeStatistics>> ProbeResults;
    std::optional<THashSet<int>> SelectedPeers;
};

////////////////////////////////////////////////////////////////////////////////

class TTestDataNodeService
    : public NRpc::TServiceBase
{
public:
    explicit TTestDataNodeService(IInvokerPtr invoker)
        : NRpc::TServiceBase(
            std::move(invoker),
            TDataNodeServiceProxy::GetDescriptor(),
            NLogging::TLogger("Test"))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProbeBlockSet));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockSet));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockRange));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta));
    }

    void ReplyWithFatalError()
    {
        if (HasFatalError_.load()) {
            THROW_ERROR_EXCEPTION(TError("Unknown error"));
        }
    }

    template <class TContext, class TResponse>
    bool ReplyWithThrottle(
        const TIntrusivePtr<TContext>& context,
        TResponse* response)
    {
        ReplyWithFatalError();
        if (EnablePartialThrottling_.load() && (Generator_.Generate<unsigned long>() % 3 == 0)) {
            response->set_net_throttling(true);
            context->Reply();
            return true;
        } else {
            return false;
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ProbeBlockSet)
    {
        if (PostponeProbingReply_.load()) {
            TDelayedExecutor::WaitForDuration(TDuration::Seconds(5));
        }

        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        response->set_has_complete_chunk(ChunkBlocks_.contains(chunkId));

        if (ReplyWithThrottle(context, response)) {
            return;
        }

        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());

        bool hasCompleteChunk = ChunkBlocks_.contains(chunkId);
        response->set_has_complete_chunk(hasCompleteChunk);

        if (ProbeStatistics_) {
            response->set_disk_queue_size(ProbeStatistics_->DiskQueueSize);
            response->set_net_queue_size(ProbeStatistics_->NetQueueSize);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockSet)
    {
        SetCalled();

        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        response->set_has_complete_chunk(ChunkBlocks_.contains(chunkId));

        if (ReplyWithThrottle(context, response)) {
            return;
        }

        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());
        auto blocks = GetBlocksByIndexes(chunkId, blockIndexes);

        ToProto(response->mutable_chunk_reader_statistics(), GetChunkReaderStatistics(blocks));

        SetRpcAttachedBlocks(response, blocks);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockRange)
    {
        SetCalled();

        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        response->set_has_complete_chunk(ChunkBlocks_.contains(chunkId));

        if (ReplyWithThrottle(context, response)) {
            return;
        }

        int firstBlockIndex = request->first_block_index();
        int blockCount = request->block_count();

        std::vector<int> blockIndexes;
        blockIndexes.resize(blockCount);

        for (int index = 0; index < blockCount; index++) {
            blockIndexes[index] = firstBlockIndex + index;
        }

        auto blocks = GetBlocksByIndexes(chunkId, blockIndexes);

        ToProto(response->mutable_chunk_reader_statistics(), GetChunkReaderStatistics(blocks));

        SetRpcAttachedBlocks(response, blocks);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkMeta)
    {
        SetCalled();

        if (ReplyWithThrottle(context, response)) {
            return;
        }

        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto metaIt = ChunkMetas_.find(chunkId);

        if (metaIt.IsEnd()) {
            THROW_ERROR_EXCEPTION("Chunk meta not found (ChunkId: %v)", chunkId);
        }

        *response->mutable_chunk_meta() = metaIt->second;
        context->Reply();
    }

    void SetChunkBlocks(
        TChunkId chunkId,
        const std::vector<TSharedRef>& blocks)
    {
        THashMap<int, TSharedRef> blockMap;
        for (int index = 0; index < std::ssize(blocks); index++) {
            blockMap.emplace(index, blocks[index]);
        }

        ChunkBlocks_.emplace(chunkId, std::move(blockMap));
    }

    void SetPartiallyResponse(bool enablePartiallyResponse)
    {
        EnablePartiallyResponse_.store(enablePartiallyResponse);
    }

    void SetFatalError()
    {
        HasFatalError_.store(true);
    }

    void SetPartiallyThrottle(bool enablePartiallyThrottle)
    {
        EnablePartialThrottling_.store(enablePartiallyThrottle);
    }

    void SetPostponeProbingReply(bool postponeProbingReply)
    {
        PostponeProbingReply_.store(postponeProbingReply);
    }

    void SetChunkMeta(
        TChunkId chunkId,
        NProto::TChunkMeta chunkMeta)
    {
        ChunkMetas_.emplace(chunkId, std::move(chunkMeta));
    }

    void SetProbeStatistics(TProbeStatistics probeStatistics)
    {
        ProbeStatistics_ = std::move(probeStatistics);
    }

    void SetCalled()
    {
        PeerCalled_ = true;
    }

    bool IsCalled()
    {
        return PeerCalled_;
    }

private:
    THashMap<TChunkId, THashMap<int, TSharedRef>> ChunkBlocks_;
    THashMap<TChunkId, NProto::TChunkMeta> ChunkMetas_;

    std::atomic<bool> EnablePartiallyResponse_ = false;
    std::atomic<bool> EnablePartialThrottling_ = false;
    std::atomic<bool> HasFatalError_ = false;
    std::atomic<bool> PostponeProbingReply_ = false;
    std::optional<TProbeStatistics> ProbeStatistics_;
    bool PeerCalled_ = false;

    TRandomGenerator Generator_{42};

    std::vector<TBlock> GetBlocksByIndexes(
        TChunkId chunkId,
        std::vector<int> blockIndexes)
    {
        auto blockChunksIt = ChunkBlocks_.find(chunkId);

        std::vector<TBlock> blocks;

        if (blockChunksIt.IsEnd()) {
            blocks.resize(blockIndexes.size());
        } else {
            auto& blockMap = blockChunksIt->second;
            blocks.reserve(blockIndexes.size());

            for (int index = 0; index < std::ssize(blockIndexes); index++) {
                if (auto it = blockMap.find(blockIndexes[index])) {
                    blocks.push_back(TBlock(it->second));
                } else {
                    blocks.push_back(TBlock());
                }
            }
        }

        if (EnablePartiallyResponse_.load()) {
            if (Generator_.Generate<ui64>() % 2 == 0) {
                return {blocks[0]};
            } else {
                for (auto& block : blocks) {
                    if (Generator_.Generate<ui64>() % 2 == 0) {
                        block = TBlock();
                    }
                }

                return blocks;
            }
        } else {
            return blocks;
        }
    }

    TChunkReaderStatisticsPtr GetChunkReaderStatistics(const std::vector<TBlock>& blocks)
    {
        auto statistics = New<TChunkReaderStatistics>();
        for (const auto& block : blocks) {
            statistics->DataBytesReadFromDisk += block.Size();
        }
        return statistics;
    }
};

////////////////////////////////////////////////////////////////////////////////

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

std::vector<TSharedRef> CreateBlocks(int count, TRandomGenerator* generator)
{
    std::vector<TSharedRef> blocks;
    blocks.reserve(count);

    for (int index = 0; index < count; index++) {
        int size = 10 + generator->Generate<ui32>() % 11;
        blocks.push_back(TSharedRef::FromString(GenerateRandomString(size, generator)));
    }

    return blocks;
}

TChunkReaderHostPtr GetChunkReaderHost(
    const NApi::NNative::IConnectionPtr connection,
    INodeStatusDirectoryPtr nodeStatusDirectory)
{
    auto localDescriptor = NNodeTrackerClient::TNodeDescriptor(
        {std::pair("default", "localhost")},
        "localhost",
        /*rack*/ {},
        /*dc*/ {});
    return New<TChunkReaderHost>(
        connection->CreateNativeClient(NApi::NNative::TClientOptions::FromUser("user")),
        std::move(localDescriptor),
        CreateClientBlockCache(
            New<TBlockCacheConfig>(),
            EBlockType::CompressedData,
            GetNullMemoryUsageTracker()),
        /*chunkMetaCache*/ nullptr,
        std::move(nodeStatusDirectory),
        /*bandwidthThrottlerProvider*/ TPerCategoryThrottlerProvider(),
        /*rpsThrottler*/ nullptr,
        /*mediumThrottler*/ nullptr,
        /*trafficMeter*/ nullptr);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

class TReplicationReaderTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TTestCase>
{ };

TEST_P(TReplicationReaderTest, ReadTest)
{
    auto testCase = GetParam();

    auto nodeCount = testCase.NodeCount;
    auto blockCount = testCase.BlockCount;
    auto batchCount = testCase.BatchCount;
    auto requestInBatch = testCase.RequestInBatch;
    auto sequentially = testCase.Sequentially;
    auto probeResults = testCase.ProbeResults;
    auto selectedPeers = testCase.SelectedPeers;

    if (probeResults || selectedPeers) {
        EXPECT_TRUE(probeResults && selectedPeers && testCase.EnableChunkProber && std::ssize(*probeResults) == nodeCount);
    }

    auto pool = NConcurrency::CreateThreadPool(16, "Worker");
    auto invoker = pool->GetInvoker();
    auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    auto memoryTracker = CreateNodeMemoryTracker(32_MB, New<TNodeMemoryTrackerConfig>(), {});

    std::vector<TIntrusivePtr<TTestDataNodeService>> services;
    THashMap<std::string, IServicePtr> addressToService;

    auto chunkId = NObjectClient::MakeRandomId(NObjectClient::EObjectType::Chunk, NObjectClient::TCellTag(0xf003));

    TRandomGenerator generator(42);
    auto blocks = CreateBlocks(blockCount, &generator);

    TChunkReplicaList replicas;
    int nodesToBan = testCase.PartiallyBanns ? nodeCount / 3 : 0;

    for (int index = 0; index < nodeCount; ++index) {
        // TODO(babenko): switch to std::string
        auto address = std::string(Format("local:%v", index));
        nodeDirectory->AddDescriptor(
            NNodeTrackerClient::TNodeId(index),
            NNodeTrackerClient::TNodeDescriptor(address));

        services.push_back(New<TTestDataNodeService>(invoker));
        auto service = services.back();
        addressToService[address] = service;
        service->SetChunkBlocks(chunkId, blocks);
        service->SetPartiallyThrottle(testCase.PartiallyThrottling);

        if (nodesToBan > 0) {
            nodesToBan--;
            service->SetFatalError();
        }

        if (!testCase.PartialPeerProbingTimeouts.empty() && index == 0) {
            service->SetPostponeProbingReply(true);
        }

        if (probeResults) {
            service->SetProbeStatistics((*probeResults)[index]);
        }

        replicas.emplace_back(NNodeTrackerClient::TNodeId(index), index);
    }

    auto options = New<TRemoteReaderOptions>();
    options->AllowFetchingSeedsFromMaster = false;

    auto channelFactory = CreateTestChannelFactory(addressToService, THashMap<std::string, IServicePtr>());
    auto connection = CreateConnection(
        std::move(channelFactory),
        {"default"},
        std::move(nodeDirectory),
        invoker,
        memoryTracker);

    EXPECT_CALL(*connection, CreateNativeClient).WillRepeatedly([&connection, &memoryTracker] (const NApi::NNative::TClientOptions& options) -> NApi::NNative::IClientPtr
        {
            return New<NApi::NNative::TClient>(connection, options, memoryTracker);
        });
    EXPECT_CALL(*connection, GetPrimaryMasterCellId).Times(testing::AtLeast(1));
    EXPECT_CALL(*connection, GetClusterDirectory).Times(testing::AtLeast(1));
    EXPECT_CALL(*connection, SubscribeReconfigured).Times(testing::AtLeast(1));
    EXPECT_CALL(*connection, UnsubscribeReconfigured).Times(testing::AtLeast(1));
    EXPECT_CALL(*connection, GetPrimaryMasterCellTag).Times(testing::AtLeast(1));
    EXPECT_CALL(*connection, GetSecondaryMasterCellTags).Times(testing::AtLeast(1));

    auto nodeStatusDirectory = testCase.MarkSomeNodesSuspicious
        ? New<TTestNodeStatusDirectory>()
        : nullptr;

    auto readerHost = GetChunkReaderHost(connection, nodeStatusDirectory);

    auto config = New<TReplicationReaderConfig>();
    config->UseChunkProber = testCase.EnableChunkProber;
    config->UseReadBlocksBatcher = testCase.EnableChunkProber;
    config->BackoffTimeMultiplier = 1.01;
    config->MinBackoffTime = TDuration::MilliSeconds(1);
    config->MaxBackoffTime = TDuration::MilliSeconds(1);
    config->BlockSetSubrequestThreshold = testCase.BlockSetSubrequestThreshold;
    config->PartialPeerProbingTimeouts = testCase.PartialPeerProbingTimeouts;
    config->Postprocess();

    auto reader = CreateReplicationReader(
        std::move(config),
        std::move(options),
        std::move(readerHost),
        chunkId,
        std::move(replicas));

    i64 minBytesToRead = 0;
    i64 maxBytesToRead = 0;
    std::vector<bool> requestedBlockMask(blockCount);

    IChunkReader::TReadBlocksOptions readBlockOptions;
    for (int batchIndex = 0; batchIndex < batchCount; batchIndex++) {
        std::vector<TFuture<std::vector<TBlock>>> futures;

        int requestSize = std::max(std::max(blockCount / requestInBatch, 1), testCase.BlockInRequest);

        for (int requestIndex = 0; requestIndex < requestInBatch; requestIndex++) {
            if (testCase.MarkSomeNodesSuspicious) {
                int suspiciousNodeCount = requestIndex % (nodeCount + 1);
                nodeStatusDirectory->SetSuspicousNodeCount(suspiciousNodeCount);
            }

            std::vector<int> blockIndexes;
            int blockInRequestCount = std::max<int>(generator.Generate<ui64>() % requestSize, 1);
            for (int blockIndex = 0; blockIndex < blockInRequestCount; blockIndex++) {
                if (sequentially) {
                    blockIndexes.push_back((requestIndex + blockIndex) % blockCount);
                } else {
                    blockIndexes.push_back(generator.Generate<ui64>() % blockCount);
                }
            }

            // Blocks may be requested in any order. Shuffle block indexes.
            for (int index = 0; index < std::ssize(blockIndexes); ++index) {
                std::swap(blockIndexes[index], blockIndexes[generator.Generate<ui64>() % (index + 1)]);
            }

            auto estimatedBytesToRead = 0;
            for (int blockIndex : blockIndexes) {
                maxBytesToRead += blocks[blockIndex].Size();
                estimatedBytesToRead += blocks[blockIndex].Size();
                if (!requestedBlockMask[blockIndex]) {
                    requestedBlockMask[blockIndex] = true;
                    minBytesToRead += blocks[blockIndex].Size();
                }
            }

            readBlockOptions.EstimatedSize = estimatedBytesToRead;

            auto future = reader->ReadBlocks(readBlockOptions, blockIndexes)
                .Apply(BIND([=] (const std::vector<TBlock>& returnedBlocks) {
                    EXPECT_EQ(blockIndexes.size(), returnedBlocks.size());
                    int index = 0;
                    for (const auto& block : returnedBlocks) {
                        EXPECT_EQ(TBlock(blocks[blockIndexes[index]]).GetOrComputeChecksum(), block.GetOrComputeChecksum());
                        index++;
                    }
                    return returnedBlocks;
                }).AsyncVia(invoker));
            futures.push_back(std::move(future));
        }

        WaitFor(AllSucceeded(std::move(futures)))
            .ThrowOnError();
    }

    if (selectedPeers) {
        for (int index = 0; index < std::ssize(services); ++index) {
            EXPECT_EQ(selectedPeers->contains(index), services[index]->IsCalled());
        }
    }

    auto statistics = readBlockOptions.ClientOptions.ChunkReaderStatistics;

    EXPECT_GE(statistics->DataBytesReadFromDisk, minBytesToRead);
    EXPECT_LE(statistics->DataBytesReadFromDisk, maxBytesToRead);

    pool->Shutdown();
    memoryTracker->ClearTrackers();
}

INSTANTIATE_TEST_SUITE_P(
    TReplicationReaderTest,
    TReplicationReaderTest,
    ::testing::Values(
        TTestCase{},
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = false,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = false,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .EnableChunkProber = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = false,
            .PartiallyThrottling = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
            .EnableChunkProber = false,
            .PartiallyThrottling = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
            .EnableChunkProber = true,
            .PartiallyThrottling = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = true,
            .PartiallyThrottling = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = false,
            .PartiallyBanns = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
            .EnableChunkProber = false,
            .PartiallyBanns = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
            .EnableChunkProber = true,
            .PartiallyBanns = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = true,
            .PartiallyBanns = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = false,
            .PartiallyThrottling = true,
            .PartiallyBanns = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
            .EnableChunkProber = false,
            .PartiallyThrottling = true,
            .PartiallyBanns = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
            .EnableChunkProber = true,
            .PartiallyThrottling = true,
            .PartiallyBanns = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = true,
            .PartiallyThrottling = true,
            .PartiallyBanns = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = false,
            .BlockSetSubrequestThreshold = 4,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
            .EnableChunkProber = false,
            .BlockSetSubrequestThreshold = 4,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
            .EnableChunkProber = true,
            .BlockSetSubrequestThreshold = 4,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = true,
            .BlockSetSubrequestThreshold = 4,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = false,
            .PartiallyThrottling = true,
            .BlockSetSubrequestThreshold = 240,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
            .EnableChunkProber = false,
            .PartiallyThrottling = true,
            .BlockSetSubrequestThreshold = 4,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
            .EnableChunkProber = true,
            .PartiallyThrottling = true,
            .BlockSetSubrequestThreshold = 240,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = true,
            .PartiallyThrottling = true,
            .BlockSetSubrequestThreshold = 4,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = false,
            .PartiallyBanns = true,
            .BlockSetSubrequestThreshold = 240,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
            .EnableChunkProber = false,
            .PartiallyBanns = true,
            .BlockSetSubrequestThreshold = 4,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
            .EnableChunkProber = true,
            .PartiallyBanns = true,
            .BlockSetSubrequestThreshold = 240,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = true,
            .PartiallyBanns = true,
            .BlockSetSubrequestThreshold = 4,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = false,
            .PartiallyThrottling = true,
            .PartiallyBanns = true,
            .BlockSetSubrequestThreshold = 240,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
            .EnableChunkProber = false,
            .PartiallyThrottling = true,
            .PartiallyBanns = true,
            .BlockSetSubrequestThreshold = 4,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
            .EnableChunkProber = true,
            .PartiallyThrottling = true,
            .PartiallyBanns = true,
            .BlockSetSubrequestThreshold = 240,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = true,
            .PartiallyThrottling = true,
            .PartiallyBanns = true,
            .BlockSetSubrequestThreshold = 4,
        },
        TTestCase{
            .NodeCount = 3,
            .Sequentially = true,
            .PartialPeerProbingTimeouts = std::vector<std::pair<int, TDuration>>{
                std::make_pair(2, TDuration::Seconds(1)),
            },

        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = false,
            .MarkSomeNodesSuspicious = true,
        },
        TTestCase{
            .NodeCount = 3,
            .Sequentially = true,
            .PartialPeerProbingTimeouts = std::vector<std::pair<int, TDuration>>{
                std::make_pair(2, TDuration::Seconds(1)),
            },
        },
        TTestCase{
            .NodeCount = 3,
            .Sequentially = true,
            .PartialPeerProbingTimeouts = std::vector<std::pair<int, TDuration>>{
                std::make_pair(2, TDuration::Seconds(1)),
            },
            .MarkSomeNodesSuspicious = true,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = true,
            .ProbeResults = {{
                TProbeStatistics{
                    .DiskQueueSize = 1,
                },
                TProbeStatistics{
                    .DiskQueueSize = 10,
                },
                TProbeStatistics{
                    .DiskQueueSize = 100,
                }
            }},
            .SelectedPeers = {{0}}
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = true,
            .EnableChunkProber = true,
            .ProbeResults = {{
                TProbeStatistics{
                    .DiskQueueSize = 10,
                    .NetQueueSize = 1000
                },
                TProbeStatistics{
                    .DiskQueueSize = 100,
                    .NetQueueSize = 100
                },
                TProbeStatistics{
                    .DiskQueueSize = 1000,
                    .NetQueueSize = 1000
                }
            }},
            .SelectedPeers = {{1}}
        }
    ));

////////////////////////////////////////////////////////////////////////////////

TEST(TReplicationReaderConfigTest, ValidateAdaptiveProbingConfig)
{
    auto config = New<TReplicationReaderConfig>();
    config->PartialPeerProbingTimeouts = std::vector<std::pair<int, TDuration>>{
        std::make_pair(1, TDuration::Seconds(1)),
    };
    config->Postprocess();
    config->PartialPeerProbingTimeouts = std::vector<std::pair<int, TDuration>>{
        std::make_pair(1, TDuration::Seconds(2)),
        std::make_pair(2, TDuration::Seconds(1)),
    };
    config->Postprocess();

    config->PartialPeerProbingTimeouts = std::vector<std::pair<int, TDuration>>{
        std::make_pair(1, TDuration::Seconds(1)),
        std::make_pair(1, TDuration::Seconds(2)),
    };
    EXPECT_THROW(config->Postprocess(), NYT::TErrorException);
    config->PartialPeerProbingTimeouts = std::vector<std::pair<int, TDuration>>{
        std::make_pair(1, TDuration::Seconds(2)),
        std::make_pair(1, TDuration::Seconds(1)),
    };
    EXPECT_THROW(config->Postprocess(), NYT::TErrorException);

    config->PartialPeerProbingTimeouts = std::vector<std::pair<int, TDuration>>{
        std::make_pair(2, TDuration::Seconds(1)),
        std::make_pair(1, TDuration::Seconds(1)),
    };
    EXPECT_THROW(config->Postprocess(), NYT::TErrorException);
    config->PartialPeerProbingTimeouts = std::vector<std::pair<int, TDuration>>{
        std::make_pair(1, TDuration::Seconds(1)),
        std::make_pair(2, TDuration::Seconds(1)),
    };
    EXPECT_THROW(config->Postprocess(), NYT::TErrorException);

    config->PartialPeerProbingTimeouts = std::vector<std::pair<int, TDuration>>{
        std::make_pair(1, TDuration::Seconds(1)),
        std::make_pair(2, TDuration::Seconds(2)),
    };
    EXPECT_THROW(config->Postprocess(), NYT::TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
