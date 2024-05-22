#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>
#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

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
#include <yt/yt/core/misc/memory_usage_tracker.h>

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

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TTestDataNodeService
    : public NRpc::TServiceBase
{
public:
    explicit TTestDataNodeService(IInvokerPtr invoker)
        : NRpc::TServiceBase(
            std::move(invoker),
            TDataNodeServiceProxy::GetDescriptor(),
            NLogging::TLogger("Test"),
            NullRealmId)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ProbeBlockSet));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockSet));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockRange));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta));
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ProbeBlockSet)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());

        bool hasCompleteChunk = ChunkBlocks_.contains(chunkId);
        response->set_has_complete_chunk(hasCompleteChunk);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockSet)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());

        auto hasCompleteChunk = ChunkBlocks_.contains(chunkId);
        response->set_has_complete_chunk(hasCompleteChunk);

        SetRpcAttachedBlocks(response, GetBlocksByIndexes(chunkId, blockIndexes));
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockRange)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        int firstBlockIndex = request->first_block_index();
        int blockCount = request->block_count();

        auto hasCompleteChunk = ChunkBlocks_.contains(chunkId);
        response->set_has_complete_chunk(hasCompleteChunk);

        std::vector<int> blockIndexes;
        blockIndexes.resize(blockCount);

        for (int index = 0; index < blockCount; index++) {
            blockIndexes[index] = firstBlockIndex + index;
        }

        SetRpcAttachedBlocks(response, GetBlocksByIndexes(chunkId, blockIndexes));
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkMeta)
    {
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
        std::vector<TSharedRef> blocks)
    {
        THashMap<int, TSharedRef> blockMap;
        for (int index = 0; index < std::ssize(blocks); index++) {
            blockMap.emplace(index, blocks[index]);
        }

        ChunkBlocks_.emplace(chunkId, std::move(blockMap));
    }

    void SetPartitialResponse(bool enablePartitialResponse)
    {
        EnablePartitialResponse_.store(enablePartitialResponse);
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

    std::atomic<bool> EnablePartitialResponse_ = false;

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
                if (auto it = blockMap.find(index)) {
                    blocks.push_back(TBlock(it->second));
                } else {
                    blocks.push_back(TBlock());
                }
            }
        }

        if (EnablePartitialResponse_.load()) {
            return {blocks[0]};
        } else {
            return blocks;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TString GenerateRandomString(size_t size)
{
    TRandomGenerator generator(42);
    TString result;
    result.reserve(size + sizeof(ui64));
    while (result.size() < size) {
        ui64 value = generator.Generate<ui64>();
        result += TStringBuf(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    result.resize(size);
    return result;
}

std::vector<TSharedRef> CreateBlocks(int count)
{
    std::vector<TSharedRef> blocks;
    blocks.reserve(count);

    for (int index = 0; index < count; index++) {
        blocks.push_back(TSharedRef::FromString(GenerateRandomString(16)));
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
        connection->CreateNativeClient({ .User = "user" }),
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

TEST(TReplicationReaderTest, SimpleReadTest)
{
    auto pool = NConcurrency::CreateThreadPool(4, "Worker");
    auto invoker = pool->GetInvoker();
    auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    auto memoryTracker = CreateNodeMemoryTracker(32_MB, {});

    for (int index = 0; index < 3; ++index) {
        nodeDirectory->AddDescriptor(
            NNodeTrackerClient::TNodeId(index),
            NNodeTrackerClient::TNodeDescriptor(Format("local:%v", index)));
    }

    auto chunkId = TGuid::Create();
    auto blocks = CreateBlocks(8);

    auto service = New<TTestDataNodeService>(invoker);
    service->SetChunkBlocks(chunkId, blocks);

    auto options = New<TRemoteReaderOptions>();
    options->AllowFetchingSeedsFromMaster = false;

    auto channelFactory = CreateTestChannelFactoryWithDefaultServices(service);
    auto connection = NApi::NNative::CreateConnection(
        std::move(channelFactory),
        { "default" },
        std::move(nodeDirectory),
        invoker,
        memoryTracker);

    auto readerHost = GetChunkReaderHost(connection);

    auto reader = CreateReplicationReader(
        New<TReplicationReaderConfig>(),
        options,
        readerHost,
        chunkId,
        TChunkReplicaWithMediumList{
            TChunkReplicaWithMedium(NNodeTrackerClient::TNodeId(0), 0, AllMediaIndex),
            TChunkReplicaWithMedium(NNodeTrackerClient::TNodeId(1), 1, AllMediaIndex),
            TChunkReplicaWithMedium(NNodeTrackerClient::TNodeId(2), 2, AllMediaIndex)
        });

    std::vector<TFuture<std::vector<TBlock>>> futures;

    for (int i = 0; i < 6; i++) {
        auto future = reader->ReadBlocks({}, {i, i + 1})
            .Apply(BIND([=] (const std::vector<TBlock>& returnedBlocks) {
                EXPECT_EQ(2, std::ssize(returnedBlocks));
                EXPECT_EQ(TBlock(blocks[i]).GetOrComputeChecksum(), returnedBlocks[0].GetOrComputeChecksum());
                EXPECT_EQ(TBlock(blocks[i + 1]).GetOrComputeChecksum(), returnedBlocks[1].GetOrComputeChecksum());
                return returnedBlocks;
            }).AsyncVia(invoker));
        futures.push_back(std::move(future));
    }

    WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();

    pool->Shutdown();
    memoryTracker->ClearTrackers();
}

TEST(TReplicationReaderTest, TestWrongAttachmentCountException)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    auto pool = NConcurrency::CreateThreadPool(4, "Worker");
    auto invoker = pool->GetInvoker();
    auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    auto memoryTracker = CreateNodeMemoryTracker(32_MB, {});

    for (int index = 0; index < 3; ++index) {
        nodeDirectory->AddDescriptor(
            NNodeTrackerClient::TNodeId(index),
            NNodeTrackerClient::TNodeDescriptor(Format("local:%v", index)));
    }

    auto chunkId = TGuid::Create();
    auto blocks = CreateBlocks(8);

    auto service = New<TTestDataNodeService>(invoker);
    service->SetPartitialResponse(true);
    service->SetChunkBlocks(chunkId, blocks);

    auto options = New<TRemoteReaderOptions>();
    options->AllowFetchingSeedsFromMaster = false;

    auto channelFactory = CreateTestChannelFactoryWithDefaultServices(service);
    auto connection = NApi::NNative::CreateConnection(
        std::move(channelFactory),
        { "default" },
        std::move(nodeDirectory),
        invoker,
        memoryTracker);

    auto readerHost = GetChunkReaderHost(connection);

    auto config = New<TReplicationReaderConfig>();
    config->PassCount = 3;
    config->RetryCount = 2;
    config->RetryTimeout = TDuration::MilliSeconds(10);

    auto reader = CreateReplicationReader(
        config,
        options,
        readerHost,
        chunkId,
        TChunkReplicaWithMediumList{
            TChunkReplicaWithMedium(NNodeTrackerClient::TNodeId(0), 0, AllMediaIndex),
            TChunkReplicaWithMedium(NNodeTrackerClient::TNodeId(1), 1, AllMediaIndex),
            TChunkReplicaWithMedium(NNodeTrackerClient::TNodeId(2), 2, AllMediaIndex)
        });

    EXPECT_EXIT(WaitFor(reader->ReadBlocks({}, {0, 1}))
        .ThrowOnError(),
        [] (int /*exitStatus*/) {
            return true;
        },
        ".*Wrong attachment count.*");
    pool->Shutdown();
    memoryTracker->ClearTrackers();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
