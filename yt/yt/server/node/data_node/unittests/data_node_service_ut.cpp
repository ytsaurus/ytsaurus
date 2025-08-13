#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/data_node_service.h>
#include <yt/yt/server/node/data_node/master_connector.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/test_framework/test_connection.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/node_tracker_client/node_tracker_service_proxy.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/test_framework/test_proxy_service.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/random.h>

#include <yt/yt/library/fusion/service_directory.h>

namespace NYT::NDataNode {

using namespace NApi;
using namespace NCellMasterClient;
using namespace NClusterNode;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNative;
using namespace NObjectClient;
using namespace NRpc;
using namespace NConcurrency;

using NYT::NBus::EMultiplexingBand;
using NNative::TConnectionDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GenerateRandomBlockIdsWithOrder(int min, int max, int count, TRandomGenerator& generator)
{
    std::vector<int> blockIds;
    blockIds.reserve(count);
    for (int i = 0; i < count; ++i) {
        blockIds.push_back(generator.Generate<unsigned>() % (max - min + 1) + min);
    }
    std::sort(blockIds.begin(), blockIds.end());
    blockIds.erase(std::unique(blockIds.begin(), blockIds.end()), blockIds.end());
    return blockIds;
}

std::vector<TChecksum> BlocksToChecksums(const std::vector<TBlock>& blocks)
{
    std::vector<TChecksum> checksums;
    checksums.reserve(blocks.size());
    for (const auto& block : blocks) {
        YT_VERIFY(block.Size() > 0);
        checksums.push_back(block.GetOrComputeChecksum());
    }
    return checksums;
}

std::string GenerateRandomString(int size, TRandomGenerator& generator)
{
    std::string result;
    result.resize(size);
    for (auto& c : result) {
        c = 'a' + (generator.Generate<unsigned>() % 26);
    }
    return result;
}

std::vector<TBlock> CreateBlocks(int count, int blockSize, TRandomGenerator& generator)
{
    std::vector<TBlock> blocks;
    blocks.reserve(count);

    for (int index = 0; index < count; ++index) {
        blocks.emplace_back(TSharedRef::FromString(GenerateRandomString(blockSize, generator)));
    }

    return blocks;
}

int CalculateCummulativeBlockSize(const std::vector<TBlock>& blocks)
{
    int size = 0;
    for (const auto& block : blocks) {
        size += block.Size();
    }
    return size;
}

////////////////////////////////////////////////////////////////////////////////

class TDataNodeBootstrapMock
    : public IBootstrap
    , public TBootstrapBase
{
public:
    TDataNodeBootstrapMock(IBootstrap* bootstrap, IMasterConnectorPtr masterConnector)
        : TBootstrapBase(bootstrap)
        , Bootstrap_(std::move(bootstrap))
        , MasterConnector_(std::move(masterConnector))
    { }

    void Initialize() override
    {
        Bootstrap_->Initialize();
    }

    void Run() override
    {
        Bootstrap_->Run();
    }

    const TChunkStorePtr& GetChunkStore() const override
    {
        return Bootstrap_->GetChunkStore();
    }

    const IAllyReplicaManagerPtr& GetAllyReplicaManager() const override
    {
        return Bootstrap_->GetAllyReplicaManager();
    }

    const TLocationManagerPtr& GetLocationManager() const override
    {
        return Bootstrap_->GetLocationManager();
    }

    const TSessionManagerPtr& GetSessionManager() const override
    {
        return Bootstrap_->GetSessionManager();
    }

    const IJobControllerPtr& GetJobController() const override
    {
        return Bootstrap_->GetJobController();
    }

    const IMasterConnectorPtr& GetMasterConnector() const override
    {
        return MasterConnector_;
    }

    const TMediumDirectoryManagerPtr& GetMediumDirectoryManager() const override
    {
        return Bootstrap_->GetMediumDirectoryManager();
    }

    const TMediumUpdaterPtr& GetMediumUpdater() const override
    {
        return Bootstrap_->GetMediumUpdater();
    }

    const NConcurrency::IThroughputThrottlerPtr& GetThrottler(EDataNodeThrottlerKind kind) const override
    {
        return Bootstrap_->GetThrottler(kind);
    }

    const NConcurrency::IThroughputThrottlerPtr& GetInThrottler(const TWorkloadDescriptor& descriptor) const override
    {
        return Bootstrap_->GetInThrottler(descriptor);
    }

    const NConcurrency::IThroughputThrottlerPtr& GetOutThrottler(const TWorkloadDescriptor& descriptor) const override
    {
        return Bootstrap_->GetOutThrottler(descriptor);
    }

    const IJournalDispatcherPtr& GetJournalDispatcher() const override
    {
        return Bootstrap_->GetJournalDispatcher();
    }

    const IInvokerPtr& GetStorageLookupInvoker() const override
    {
        return Bootstrap_->GetStorageLookupInvoker();
    }

    const IInvokerPtr& GetMasterJobInvoker() const override
    {
        return Bootstrap_->GetMasterJobInvoker();
    }

    const TP2PBlockCachePtr& GetP2PBlockCache() const override
    {
        return Bootstrap_->GetP2PBlockCache();
    }

    const TP2PSnooperPtr& GetP2PSnooper() const override
    {
        return Bootstrap_->GetP2PSnooper();
    }

    const TTableSchemaCachePtr& GetTableSchemaCache() const override
    {
        return Bootstrap_->GetTableSchemaCache();
    }

    const NQueryClient::IRowComparerProviderPtr& GetRowComparerProvider() const override
    {
        return Bootstrap_->GetRowComparerProvider();
    }

    const IIOThroughputMeterPtr& GetIOThroughputMeter() const override
    {
        return Bootstrap_->GetIOThroughputMeter();
    }

    const TLocationHealthCheckerPtr& GetLocationHealthChecker() const override
    {
        return Bootstrap_->GetLocationHealthChecker();
    }

    void SetLocationUuidsRequired(bool value) override
    {
        MasterConnector_->SetLocationUuidsRequired(value);
    }

    void SetPerLocationFullHeartbeatsEnabled(bool value) override
    {
        MasterConnector_->SetPerLocationFullHeartbeatsEnabled(value);
    }

private:
    IBootstrap* const Bootstrap_;
    const IMasterConnectorPtr MasterConnector_;
};

////////////////////////////////////////////////////////////////////////////////

struct TMasterConnectorMock
    : public IMasterConnector
{
    MOCK_METHOD(void, Initialize, (), (override));

    MOCK_METHOD(void, ScheduleHeartbeat, (), (override));

    MOCK_METHOD(void, ScheduleJobHeartbeat, (const std::string& jobTrackerAddress), (override));

    MOCK_METHOD(bool, IsOnline, (), (const, override));

    MOCK_METHOD(void, SetLocationUuidsRequired, (bool value), (override));

    MOCK_METHOD(void, SetPerLocationFullHeartbeatsEnabled, (bool value), (override));
};

////////////////////////////////////////////////////////////////////////////////

struct TCellDirectoryMock
    : public ICellDirectory
{
    DEFINE_SIGNAL_OVERRIDE(TCellReconfigurationSignature, CellDirectoryChanged);

    MOCK_METHOD(void, Update, (const NCellMasterClient::NProto::TCellDirectory& protoDirectory), (override));
    MOCK_METHOD(void, UpdateDefault, (), (override));

    MOCK_METHOD(TCellId, GetPrimaryMasterCellId, (), (override));
    MOCK_METHOD(TCellTag, GetPrimaryMasterCellTag, (), (override));
    MOCK_METHOD(TCellTagList, GetSecondaryMasterCellTags, (), (override));
    MOCK_METHOD(THashSet<TCellId>, GetSecondaryMasterCellIds, (), (override));

    MOCK_METHOD(IChannelPtr, FindMasterChannel, (EMasterChannelKind, TCellTag), (override));
    MOCK_METHOD(IChannelPtr, GetMasterChannelOrThrow, (EMasterChannelKind, TCellTag), (override));
    MOCK_METHOD(IChannelPtr, GetMasterChannelOrThrow, (EMasterChannelKind, TCellId), (override));

    MOCK_METHOD(TCellTagList, GetMasterCellTagsWithRole, (EMasterCellRole), (override));

    MOCK_METHOD(TCellId, GetRandomMasterCellWithRoleOrThrow, (EMasterCellRole), (override));

    MOCK_METHOD(bool, IsMasterCacheConfigured, (), (override));

    MOCK_METHOD(IChannelPtr, FindNakedMasterChannel, (EMasterChannelKind, TCellTag), (override));
    MOCK_METHOD(IChannelPtr, GetNakedMasterChannelOrThrow, (EMasterChannelKind, TCellTag), (override));
};

////////////////////////////////////////////////////////////////////////////////

class TTestNodeTrackerService
    : public TServiceBase
{
public:
    TTestNodeTrackerService(IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            NNodeTrackerClient::TNodeTrackerServiceProxy::GetDescriptor(),
            NLogging::TLogger("TTestNodeTrackerService"))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterNode)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat)
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, RegisterNode)
    {
        response->set_node_id(NodeCounter_++);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, Heartbeat)
    {
        context->Reply();
    }

    std::atomic<int> NodeCounter_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TThreadPoolConfig)

struct TThreadPoolConfig
    : public NYTree::TYsonStruct
{
    int ReadThreadCount;
    int WriteThreadCount;

    REGISTER_YSON_STRUCT(TThreadPoolConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("read_thread_count", &TThis::ReadThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
        registrar.Parameter("write_thread_count", &TThis::WriteThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
    }
};

DEFINE_REFCOUNTED_TYPE(TThreadPoolConfig)

////////////////////////////////////////////////////////////////////////////////

class TDataNodeTest
    : public ::testing::Test
{
public:
    struct TDataNodeTestParams
    {
        bool EnableSequentialIORequests = true;
        i64 CoalescedReadMaxGapSize = 10_MB;
        int ClusterConnectionThreadPoolSize = 4;
        int ReadThreadCount = 1;
        int WriteThreadCount = 1;
        i64 WriteMemoryLimit = 128_GB;
        i64 ReadMemoryLimit = 128_GB;
        i64 LegacyWriteMemoryLimit = 128_GB;
    };

    explicit TDataNodeTest(const TDataNodeTestParams& testParams)
        : TestParams_(testParams)
    { }

    TClusterNodeBootstrapConfigPtr GenerateClusterBootstrapConfig() const
    {
        auto bootstrapConfig = New<TClusterNodeBootstrapConfig>();
        bootstrapConfig->Flavors = {NNodeTrackerClient::ENodeFlavor::Data};
        bootstrapConfig->ClusterConnection = New<TConnectionCompoundConfig>();
        bootstrapConfig->ClusterConnection->Static = New<TConnectionStaticConfig>();
        bootstrapConfig->ClusterConnection->Static->PrimaryMaster = New<TMasterConnectionConfig>();
        bootstrapConfig->ClusterConnection->Static->PrimaryMaster->Addresses = {PrimaryMasterAddress};
        bootstrapConfig->ClusterConnection->Static->PrimaryMaster->CellId = CellId;

        bootstrapConfig->ClusterConnection->Dynamic = New<TConnectionDynamicConfig>();
        bootstrapConfig->ClusterConnection->Dynamic->ThreadPoolSize = TestParams_.ClusterConnectionThreadPoolSize;

        bootstrapConfig->DataNode = New<TDataNodeConfig>();
        bootstrapConfig->DataNode->EnableSequentialIORequests = TestParams_.EnableSequentialIORequests;

        bootstrapConfig->DataNode->MasterConnector = New<TMasterConnectorConfig>();
        bootstrapConfig->DataNode->MasterConnector->JobHeartbeatPeriod = TDuration::Seconds(1);

        bootstrapConfig->DataNode->BlockCache = New<TBlockCacheConfig>();
        auto cacheConfig = New<TSlruCacheConfig>();
        cacheConfig->Capacity = 0;
        bootstrapConfig->DataNode->BlockCache->CompressedData = cacheConfig;
        bootstrapConfig->DataNode->BlockCache->UncompressedData = cacheConfig;

        for (auto kind : TEnumTraits<EDataNodeThrottlerKind>::GetDomainValues()) {
            if (!bootstrapConfig->DataNode->Throttlers[kind]) {
                bootstrapConfig->DataNode->Throttlers[kind] = New<NConcurrency::TRelativeThroughputThrottlerConfig>();
            }
        }

        auto storeLocationConfig = New<TStoreLocationConfig>();
        storeLocationConfig->Path = StoreLocationPath_;
        storeLocationConfig->IOEngineType = NIO::EIOEngineType::ThreadPool;
        auto threadPoolConfig = New<TThreadPoolConfig>();
        threadPoolConfig->ReadThreadCount = TestParams_.ReadThreadCount;
        threadPoolConfig->WriteThreadCount = TestParams_.WriteThreadCount;
        storeLocationConfig->IOConfig = NYTree::ConvertToNode(threadPoolConfig);
        storeLocationConfig->Throttlers = {};
        storeLocationConfig->WriteMemoryLimit = TestParams_.WriteMemoryLimit;
        storeLocationConfig->ReadMemoryLimit = TestParams_.ReadMemoryLimit;
        storeLocationConfig->LegacyWriteMemoryLimit = TestParams_.LegacyWriteMemoryLimit;
        storeLocationConfig->CoalescedReadMaxGapSize = TestParams_.CoalescedReadMaxGapSize;

        for (auto kind : TEnumTraits<EChunkLocationThrottlerKind>::GetDomainValues()) {
            if (!storeLocationConfig->Throttlers[kind]) {
                storeLocationConfig->Throttlers[kind] = New<NConcurrency::TRelativeThroughputThrottlerConfig>();
            }
        }

        bootstrapConfig->DataNode->StoreLocations.push_back(storeLocationConfig);

        bootstrapConfig->ResourceLimits->TotalMemory = 256_GB;
        for (auto kind : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
            bootstrapConfig->ResourceLimits->MemoryLimits[kind] = New<TMemoryLimit>();
            bootstrapConfig->ResourceLimits->MemoryLimits[kind]->Value = 128_GB;
        }

        return bootstrapConfig;
    }

    void SetUp() override
    {
        TRandomGenerator generator(TInstant::Now().MicroSeconds());
        StoreLocationPath_ = Format("/tmp/%v/chunk_store", GenerateRandomString(5, generator));
        ActionQueue_ = New<NConcurrency::TActionQueue>();
        auto nodeTrackerService = New<TTestNodeTrackerService>(ActionQueue_->GetInvoker());

        CellDirectoryMock_ = New<TCellDirectoryMock>();

        auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        nodeDirectory->AddDescriptor(
            NYT::NNodeTrackerClient::TNodeId(0),
            NNodeTrackerClient::TNodeDescriptor(std::string(PrimaryMasterAddress)));

        auto masterChannelFactory = CreateTestChannelFactory(
            THashMap<std::string, IServicePtr>{{PrimaryMasterAddress, nodeTrackerService}},
            THashMap<std::string, IServicePtr>());

        MemoryTracker_ = CreateNodeMemoryTracker(32_MB, New<TNodeMemoryTrackerConfig>(), {});

        TestConnection_ = CreateConnection(
            std::move(masterChannelFactory),
            /*networkPreferenceList*/ {"default"},
            std::move(nodeDirectory),
            ActionQueue_->GetInvoker(),
            MemoryTracker_);

        EXPECT_CALL(*TestConnection_, CreateNativeClient).WillRepeatedly([this] (const NApi::TClientOptions& options) -> NApi::NNative::IClientPtr
            {
                return New<TClient>(TestConnection_, options, MemoryTracker_);
            });
        EXPECT_CALL(*TestConnection_, GetClusterDirectory).WillRepeatedly(testing::DoDefault());
        EXPECT_CALL(*TestConnection_, SubscribeReconfigured).WillRepeatedly(testing::DoDefault());
        EXPECT_CALL(*TestConnection_, GetPrimaryMasterCellTag()).WillRepeatedly([] () -> TCellTag {
            return TCellTag(0xf003);
        });
        EXPECT_CALL(*TestConnection_, GetMasterCellId).WillRepeatedly([] () -> TCellId {
            return CellId;
        });
        EXPECT_CALL(*CellDirectoryMock_, GetPrimaryMasterCellId).WillRepeatedly([] () -> TCellId {
            return CellId;
        });
        EXPECT_CALL(*TestConnection_, GetPrimaryMasterCellId).WillRepeatedly([] () -> TCellId {
            return CellId;
        });
        EXPECT_CALL(*TestConnection_, GetMasterCellDirectory()).WillRepeatedly([this] () -> ICellDirectoryPtr {
            return CellDirectoryMock_;
        });
        EXPECT_CALL(*TestConnection_, GetSecondaryMasterCellTags()).WillRepeatedly([] () -> TCellTagList {
            return {};
        });

        ClusterNodeBootstrap_ = NClusterNode::CreateNodeBootstrap(
            GenerateClusterBootstrapConfig(),
            NYTree::GetEphemeralNodeFactory(false)->CreateMap(),
            NFusion::CreateServiceDirectory(),
            TestConnection_);
        ClusterNodeBootstrap_->Initialize();

        MasterConnectorMock_ = New<TMasterConnectorMock>();
        EXPECT_CALL(*MasterConnectorMock_, IsOnline()).WillRepeatedly(testing::Return(true));
        DataNodeBootstrap_ = New<TDataNodeBootstrapMock>(ClusterNodeBootstrap_->GetDataNodeBootstrap(), MasterConnectorMock_);

        if (CellDirectoryMock_) {
            testing::Mock::AllowLeak(CellDirectoryMock_.Get());
        }

        if (MasterConnectorMock_) {
            testing::Mock::AllowLeak(MasterConnectorMock_.Get());
        }

        if (TestConnection_) {
            testing::Mock::AllowLeak(TestConnection_.Get());
        }

        DataNodeService_ = CreateDataNodeService(DataNodeBootstrap_->GetConfig()->DataNode, DataNodeBootstrap_.Get());
        ChannelFactory_ = CreateTestChannelFactory(
            THashMap<std::string, IServicePtr>{{DataNodeServiceAddress, DataNodeService_}},
            THashMap<std::string, IServicePtr>());
    }

    void TearDown() override
    {
        MemoryTracker_->ClearTrackers();
        MemoryTracker_.Reset();
        WorkloadDescriptor_ = {};
        ChannelFactory_.Reset();
        DataNodeService_->Stop().Wait();
        DataNodeService_.Reset();
        DataNodeBootstrap_.Reset();
        ClusterNodeBootstrap_.Reset();
        ActionQueue_->Shutdown(true);
        ActionQueue_.Reset();
        CellDirectoryMock_.Reset();
        MasterConnectorMock_.Reset();
        TestConnection_.Reset();
    }

    const NDataNode::IBootstrapPtr& GetDataNodeBootstrap() const {
        return DataNodeBootstrap_;
    }

    const NConcurrency::TActionQueuePtr& GetActionQueue() const
    {
        return ActionQueue_;
    }

    auto StartChunk(const TSessionId& sessionId, bool useProbePutBlocks)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.StartChunk();
        req->set_use_probe_put_blocks(useProbePutBlocks);
        ToProto(req->mutable_session_id(), sessionId);
        SetRequestWorkloadDescriptor(req, WorkloadDescriptor_);

        return req->Invoke();
    }

    auto PutBlocks(const TSessionId& sessionId, const std::vector<TBlock>& blocks, int firstBlockIndex, i64 cumulativeBlockSize)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.PutBlocks();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        ToProto(req->mutable_session_id(), sessionId);
        req->set_first_block_index(firstBlockIndex);
        req->set_cumulative_block_size(cumulativeBlockSize);
        req->SetTimeout(RequestTimeout_);
        SetRpcAttachedBlocks(req, blocks);

        return req->Invoke();
    }

    auto FlushBlocks(const TSessionId& sessionId, int blockIndex)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.FlushBlocks();
        req->set_block_index(blockIndex);
        req->SetTimeout(RequestTimeout_);
        ToProto(req->mutable_session_id(), sessionId);

        return req->Invoke();
    }

    auto FinishChunk(const TSessionId& sessionId, int blockCount)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.FinishChunk();
        auto deferredMeta = New<TDeferredChunkMeta>();
        deferredMeta->set_type(ToProto(EChunkType::File));
        deferredMeta->set_format(ToProto(EChunkFormat::FileDefault));
        *deferredMeta->mutable_extensions() = {};
        *req->mutable_chunk_meta() = *deferredMeta;
        req->set_block_count(blockCount);
        req->SetTimeout(RequestTimeout_);
        ToProto(req->mutable_session_id(), sessionId);

        return req->Invoke();
    }

    auto CancelChunk(const TSessionId& sessionId)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.CancelChunk();
        req->SetTimeout(RequestTimeout_);
        ToProto(req->mutable_session_id(), sessionId);

        return req->Invoke();
    }

    auto GetBlockSet(
        const TChunkId& chunkId,
        const std::vector<int>& blockIndices,
        bool populateCache,
        bool fetchFromCache,
        bool fetchFromDisk)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.GetBlockSet();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        req->SetMultiplexingParallelism(1);
        SetRequestWorkloadDescriptor(req, WorkloadDescriptor_);
        ToProto(req->mutable_chunk_id(), chunkId);
        for (auto blockIndex : blockIndices) {
            req->add_block_indexes(blockIndex);
        }
        req->Header().set_timeout(ToProto(RequestTimeout_));
        req->Header().set_start_time(ToProto(TInstant::Now()));
        req->SetTimeout(RequestTimeout_);
        req->set_populate_cache(populateCache);
        req->set_fetch_from_cache(fetchFromCache);
        req->set_fetch_from_disk(fetchFromDisk);

        TGuid readSessionId{};
        readSessionId.Parts32[0] = Counter_++;
        ToProto(req->mutable_read_session_id(), readSessionId);

        return req->Invoke();
    }

    std::vector<TBlock> FillWithRandomBlocks(TSessionId sessionId, int blockCount, int blockSize)
    {
        auto blocks = CreateBlocks(blockCount, blockSize, Generator_);
        auto cummulativeBlockSize = CalculateCummulativeBlockSize(blocks);
        WaitFor(StartChunk(sessionId, false)).ThrowOnError();
        WaitFor(PutBlocks(sessionId, blocks, 0, cummulativeBlockSize)).ThrowOnError();
        WaitFor(FlushBlocks(sessionId, blockCount - 1)).ThrowOnError();
        WaitFor(FinishChunk(sessionId, blockCount)).ThrowOnError();
        return blocks;
    }

private:
    static constexpr TCellId CellId{0xa0f7b159, 0x17c04060, 0x41a0259, 0x46d5f0b7};
    static constexpr const char* DataNodeServiceAddress = "local:1045";
    static constexpr const char* PrimaryMasterAddress = "local:1081";

    const TDuration RequestTimeout_ = TDuration::Seconds(30);
    const TDataNodeTestParams TestParams_;

    TRandomGenerator Generator_{42};

    std::atomic<int> Counter_ = 0;

    INodeMemoryTrackerPtr MemoryTracker_;
    TString StoreLocationPath_;
    NConcurrency::TActionQueuePtr ActionQueue_;
    NClusterNode::IBootstrapPtr ClusterNodeBootstrap_;
    NDataNode::IBootstrapPtr DataNodeBootstrap_;
    IServicePtr DataNodeService_;
    IChannelFactoryPtr ChannelFactory_;
    TWorkloadDescriptor WorkloadDescriptor_;
    TIntrusivePtr<TCellDirectoryMock> CellDirectoryMock_;
    TIntrusivePtr<TMasterConnectorMock> MasterConnectorMock_;
    TIntrusivePtr<TTestConnection> TestConnection_;
};

////////////////////////////////////////////////////////////////////////////////

struct TGetBlockSetTestCase
{
    int BlockCount = 100;
    int BlockSize = 1_MB;
    int ParallelGetBlockSetCount = 1;
    int BlocksInRequest = 10;
    bool PopulateCache = true;
    bool FetchFromCache = true;
    bool FetchFromDisk = true;
    bool EnableSequentialIORequests = true;
};

////////////////////////////////////////////////////////////////////////////////

class TGetBlockSetTest
    : public TDataNodeTest
    , public ::testing::WithParamInterface<TGetBlockSetTestCase>
{
public:
    TGetBlockSetTest()
        : TDataNodeTest(
            TDataNodeTest::TDataNodeTestParams {
                .EnableSequentialIORequests = GetParam().EnableSequentialIORequests,
                .ReadThreadCount = 4,
                .WriteThreadCount = 4
            })
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TGetBlockSetGapTestCase
{
    int BlockCount = 40;
    int BlockSize = 1_MB;
    bool PopulateCache = true;
    bool FetchFromCache = true;
    bool FetchFromDisk = true;
    bool EnableSequentialIORequests = true;
    i64 CoalescedReadMaxGapSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TGetBlockSetGapTest
    : public TDataNodeTest
    , public ::testing::WithParamInterface<TGetBlockSetGapTestCase>
{
public:
    TGetBlockSetGapTest()
        : TDataNodeTest(
            TDataNodeTest::TDataNodeTestParams {
                .EnableSequentialIORequests = GetParam().EnableSequentialIORequests,
                .CoalescedReadMaxGapSize = GetParam().CoalescedReadMaxGapSize,
                .ReadThreadCount = 4,
                .WriteThreadCount = 4,
            })
    { }
};

TEST_P(TGetBlockSetGapTest, GetBlocksByOneDiskIORequestTest)
{
    auto testCase = GetParam();

    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    TRandomGenerator generator(42);
    int blockCount = testCase.BlockCount;
    int blockSize = testCase.BlockSize;
    auto blocks = FillWithRandomBlocks(sessionId, blockCount, blockSize);

    std::vector<int> blockIndices(blockCount);
    if (testCase.CoalescedReadMaxGapSize >= blockSize) {
        blockIndices.resize(blockCount / 2);
        for (int i = 0; i < std::ssize(blockIndices); i++) {
            blockIndices[i] = i * 2;
        }
    } else {
        for (int i = 0; i < std::ssize(blockIndices); i++) {
            blockIndices[i] = i;
        }
    }
    std::vector<TBlock> fetchedBlocks(blockIndices.size());
    std::transform(blockIndices.begin(), blockIndices.end(), fetchedBlocks.begin(), [&] (int blockIndex) {
        return blocks[blockIndex];
    });
    auto rspOrError = WaitFor(GetBlockSet(sessionId.ChunkId, blockIndices, testCase.PopulateCache, testCase.FetchFromCache, testCase.FetchFromDisk));
    YT_VERIFY(rspOrError.IsOK());
    auto rsp = rspOrError.Value();
    auto chunkReaderStatistics = rsp->chunk_reader_statistics();
    EXPECT_EQ(chunkReaderStatistics.data_io_requests(), 1);
    auto gotBlocks = GetRpcAttachedBlocks(rsp);
    EXPECT_EQ(gotBlocks.size(), fetchedBlocks.size());
    EXPECT_EQ(BlocksToChecksums(gotBlocks), BlocksToChecksums(fetchedBlocks));
}

INSTANTIATE_TEST_SUITE_P(
    TGetBlockSetGapTest,
    TGetBlockSetGapTest,
    ::testing::Values(
        TGetBlockSetGapTestCase{
            .PopulateCache = false,
            .FetchFromCache = false,
            .FetchFromDisk = true,
            .EnableSequentialIORequests = true
        },
        TGetBlockSetGapTestCase{
            .PopulateCache = false,
            .FetchFromCache = false,
            .FetchFromDisk = true,
            .EnableSequentialIORequests = false
        },
        TGetBlockSetGapTestCase{
            .BlockSize = 1_MB,
            .PopulateCache = false,
            .FetchFromCache = false,
            .FetchFromDisk = true,
            .EnableSequentialIORequests = false,
            .CoalescedReadMaxGapSize = 1_MB,
        },
        TGetBlockSetGapTestCase{
            .BlockSize = 1_MB,
            .PopulateCache = false,
            .FetchFromCache = false,
            .FetchFromDisk = true,
            .EnableSequentialIORequests = false,
            .CoalescedReadMaxGapSize = 1_MB / 2,
        }
    )
);

TEST_P(TGetBlockSetTest, GetBlockSetTest)
{
    auto testCase = GetParam();

    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    TRandomGenerator generator(42);
    int blockCount = testCase.BlockCount;
    int blkSize = testCase.BlockSize;
    auto blocks = FillWithRandomBlocks(sessionId, blockCount, blkSize);

    int getBlockSetCount = testCase.ParallelGetBlockSetCount;
    std::vector<TFuture<void>> getBlockSetFutures;
    getBlockSetFutures.reserve(getBlockSetCount);
    for (int i = 0; i < getBlockSetCount; ++i) {
        auto blockIndices = GenerateRandomBlockIdsWithOrder(0, blocks.size() - 1, testCase.BlocksInRequest, generator);
        std::vector<TBlock> fetchedBlocks(blockIndices.size());
        for (int i = 0; i < std::ssize(blockIndices); ++i) {
            fetchedBlocks[i] = blocks[blockIndices[i]];
        }
        auto future = GetBlockSet(sessionId.ChunkId, blockIndices, testCase.PopulateCache, testCase.FetchFromCache, testCase.FetchFromDisk)
            .Apply(BIND([fetchedBlocks = std::move(fetchedBlocks)] (const TIntrusivePtr<TTypedClientResponse<TRspGetBlockSet>>& response) {
                if (response->disk_throttling() || response->net_throttling()) {
                    return;
                }
                auto gotBlocks = GetRpcAttachedBlocks(response);
                EXPECT_EQ(gotBlocks.size(), fetchedBlocks.size());
                EXPECT_EQ(BlocksToChecksums(gotBlocks), BlocksToChecksums(fetchedBlocks));
        }));
        getBlockSetFutures.push_back(std::move(future));
    }

    auto allsucceededGetBlockSetFutures = AllSucceeded(getBlockSetFutures);
    EXPECT_TRUE(allsucceededGetBlockSetFutures.Wait(TDuration::Seconds(60)));
    auto getBlockSetFuturesResult = allsucceededGetBlockSetFutures.TryGet();
    EXPECT_TRUE(getBlockSetFuturesResult.has_value());
    EXPECT_TRUE(getBlockSetFuturesResult.has_value() && getBlockSetFuturesResult->IsOK());
}

INSTANTIATE_TEST_SUITE_P(
    TGetBlockSetTest,
    TGetBlockSetTest,
    ::testing::Values(
        TGetBlockSetTestCase{},
        TGetBlockSetTestCase{
            .BlockCount = 100,
            .BlockSize = 1_KB,
            .ParallelGetBlockSetCount = 1,
            .BlocksInRequest = 100,
            .PopulateCache = false,
            .FetchFromCache = false,
            .FetchFromDisk = true
        },
        TGetBlockSetTestCase{
            .PopulateCache = false,
            .FetchFromCache = false,
            .FetchFromDisk = true,
            .EnableSequentialIORequests = true
        },
        TGetBlockSetTestCase{
            .PopulateCache = false,
            .FetchFromCache = false,
            .FetchFromDisk = true,
            .EnableSequentialIORequests = false
        },
        TGetBlockSetTestCase{
            .BlockCount = 1000,
            .BlockSize = 1_MB,
            .ParallelGetBlockSetCount = 100,
            .BlocksInRequest = 40,
            .PopulateCache = true,
            .FetchFromCache = true,
            .FetchFromDisk = true
        },
        TGetBlockSetTestCase{
            .BlockCount = 1000,
            .BlockSize = 1_MB,
            .ParallelGetBlockSetCount = 100,
            .BlocksInRequest = 40,
            .PopulateCache = false,
            .FetchFromCache = true,
            .FetchFromDisk = true
        },
        TGetBlockSetTestCase{
            .BlockCount = 1000,
            .BlockSize = 1_MB,
            .ParallelGetBlockSetCount = 100,
            .BlocksInRequest = 40,
            .PopulateCache = true,
            .FetchFromCache = false,
            .FetchFromDisk = true
        },
        TGetBlockSetTestCase{
            .BlockCount = 1000,
            .BlockSize = 1_MB,
            .ParallelGetBlockSetCount = 100,
            .BlocksInRequest = 40,
            .PopulateCache = false,
            .FetchFromCache = false,
            .FetchFromDisk = true
        },
        TGetBlockSetTestCase{
            .BlockCount = 1000,
            .BlockSize = 1_MB,
            .ParallelGetBlockSetCount = 100,
            .BlocksInRequest = 40,
            .PopulateCache = false,
            .FetchFromCache = false,
            .FetchFromDisk = true,
            .EnableSequentialIORequests = false
        }
    )
);

////////////////////////////////////////////////////////////////////////////////

}
