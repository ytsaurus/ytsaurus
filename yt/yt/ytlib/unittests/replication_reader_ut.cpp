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

#include <yt/yt/ytlib/unittests/misc/test_connection.h>

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

#include <library/cpp/yt/memory/memory_usage_tracker.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NRpc;

using NYT::ToProto;
using NYT::FromProto;

namespace {

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
        const TIntrusivePtr<TContext> context,
        TResponse* response)
    {
        ReplyWithFatalError();
        if (EnablePartiallyThrottle_.load() && (Generator_.Generate<unsigned long>() % 3 == 0)) {
            response->set_net_throttling(true);
            context->Reply();
            return true;
        } else {
            return false;
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ProbeBlockSet)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        response->set_has_complete_chunk(ChunkBlocks_.contains(chunkId));

        if (ReplyWithThrottle(context, response)) {
            return;
        }

        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());

        bool hasCompleteChunk = ChunkBlocks_.contains(chunkId);
        response->set_has_complete_chunk(hasCompleteChunk);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockSet)
    {
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
        EnablePartiallyThrottle_.store(enablePartiallyThrottle);
    }

    void SetChunkMeta(
        TChunkId chunkId,
        NProto::TChunkMeta chunkMeta)
    {
        ChunkMetas_.emplace(chunkId, std::move(chunkMeta));
    }

private:
    THashMap<TChunkId, THashMap<int, TSharedRef>> ChunkBlocks_;
    THashMap<TChunkId, NProto::TChunkMeta> ChunkMetas_;

    std::atomic<bool> EnablePartiallyResponse_ = false;
    std::atomic<bool> EnablePartiallyThrottle_ = false;
    std::atomic<bool> HasFatalError_ = false;
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

TChunkReaderHostPtr GetChunkReaderHost(const NApi::NNative::IConnectionPtr connection)
{
    auto localDescriptor = NNodeTrackerClient::TNodeDescriptor(
        {std::pair("default", "localhost")},
        "localhost",
        /*rack*/ {},
        /*dc*/ {});
    return New<TChunkReaderHost>(
        connection->CreateNativeClient(NApi::TClientOptions::FromUser("user")),
        std::move(localDescriptor),
        CreateClientBlockCache(
            New<TBlockCacheConfig>(),
            EBlockType::CompressedData,
            GetNullMemoryUsageTracker()),
        /*chunkMetaCache*/ nullptr,
        /*nodeStatusDirectory*/ nullptr,
        NConcurrency::GetUnlimitedThrottler(),
        NConcurrency::GetUnlimitedThrottler(),
        NConcurrency::GetUnlimitedThrottler(),
        /*trafficMeter*/ nullptr);
}

////////////////////////////////////////////////////////////////////////////////

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
};

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

    auto pool = NConcurrency::CreateThreadPool(16, "Worker");
    auto invoker = pool->GetInvoker();
    auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    auto memoryTracker = CreateNodeMemoryTracker(32_MB, {});

    THashMap<std::string, IServicePtr> addressToService;

    auto chunkId = TGuid::Create();

    TRandomGenerator generator(42);
    auto blocks = CreateBlocks(blockCount, &generator);

    TChunkReplicaWithMediumList replicaList;
    int nodeToBans = testCase.PartiallyBanns ? nodeCount / 3 : 0;

    for (int index = 0; index < nodeCount; ++index) {
        // TODO(babenko): switch to std::string
        auto address = std::string(Format("local:%v", index));
        nodeDirectory->AddDescriptor(
            NNodeTrackerClient::TNodeId(index),
            NNodeTrackerClient::TNodeDescriptor(address));

        auto service = New<TTestDataNodeService>(invoker);
        addressToService[address] = service;
        service->SetChunkBlocks(chunkId, blocks);
        service->SetPartiallyThrottle(testCase.PartiallyThrottling);

        if (nodeToBans) {
            nodeToBans--;
            service->SetFatalError();
        }

        replicaList.push_back(TChunkReplicaWithMedium(NNodeTrackerClient::TNodeId(index), index, AllMediaIndex));
    }

    auto options = New<TRemoteReaderOptions>();
    options->AllowFetchingSeedsFromMaster = false;

    auto channelFactory = CreateTestChannelFactory(addressToService, THashMap<std::string, IServicePtr>());
    auto connection = NApi::NNative::CreateConnection(
        std::move(channelFactory),
        {"default"},
        std::move(nodeDirectory),
        invoker,
        memoryTracker);

    auto readerHost = GetChunkReaderHost(connection);

    auto config = New<TReplicationReaderConfig>();
    config->UseChunkProber = testCase.EnableChunkProber;
    config->UseReadBlocksBatcher = testCase.EnableChunkProber;
    config->BackoffTimeMultiplier = 1;
    config->MinBackoffTime = TDuration::MilliSeconds(1);
    config->MaxBackoffTime = TDuration::MilliSeconds(1);

    auto reader = CreateReplicationReader(
        std::move(config),
        std::move(options),
        std::move(readerHost),
        chunkId,
        std::move(replicaList));

    i64 minBytesToRead = 0;
    i64 maxBytesToRead = 0;
    std::vector<bool> requestedBlockMask(blockCount);

    IChunkReader::TReadBlocksOptions readBlockOptions;
    for (int batchIndex = 0; batchIndex < batchCount; batchIndex++) {
        std::vector<TFuture<std::vector<TBlock>>> futures;

        int requestSize = std::max(std::max(blockCount / requestInBatch, 1), testCase.BlockInRequest);

        for (int requestIndex = 0; requestIndex < requestInBatch; requestIndex++) {
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

            for (int blockIndex : blockIndexes) {
                maxBytesToRead += blocks[blockIndex].Size();
                if (!requestedBlockMask[blockIndex]) {
                    requestedBlockMask[blockIndex] = true;
                    minBytesToRead += blocks[blockIndex].Size();
                }
            }

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
            .Sequentially = false,
            .EnableChunkProber = false,
        },
        TTestCase{
            .BatchCount = 16,
            .NodeCount = 3,
            .BlockCount = 1024,
            .RequestInBatch = 32,
            .BlockInRequest = 16,
            .Sequentially = false,
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
        }));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
